package storage

import (
	"context"
	"fmt"
	"io"
	"path"
	"os"
	"strings"
	"time"

	"github.com/anuvu/atomfs"
	"github.com/containers/image/v5/copy"
	"github.com/containers/image/v5/docker"
	"github.com/containers/image/v5/oci/layout"
	"github.com/containers/image/v5/signature"
	"github.com/containers/image/v5/types"
	"github.com/containers/image/v5/zot"
	"github.com/containers/storage/pkg/idtools"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/openSUSE/umoci"
	"github.com/openSUSE/umoci/oci/casext"
	"github.com/pkg/errors"
)

type atomfsRuntimeServer struct {
	ctx             context.Context
	graphRoot       string
	runtimeDir      string
	runtimeMetadata map[string]*RuntimeContainerMetadata
}

func newAtomfsServer(ctx context.Context, imgServer ImageServer) RuntimeServer {
	return &atomfsRuntimeServer{
		ctx,
		imgServer.GetStore().GraphRoot(),
		imgServer.GetStore().RunRoot(),
		map[string]*RuntimeContainerMetadata{},
	}
}

func (ars *atomfsRuntimeServer) CreatePodSandbox(systemContext *types.SystemContext, podName, podID, imageName, imageAuthFile, imageID, containerName, metadataName, uid, namespace string, attempt uint32, idMappings *idtools.IDMappings, labelOptions []string) (ContainerInfo, error) {
	return ars.createContainerOrPodSandbox(systemContext, podName, podID, imageName, imageAuthFile, imageID, containerName, podID, metadataName, uid, namespace, attempt, idMappings, labelOptions, true)
}

func (ars *atomfsRuntimeServer) RemovePodSandbox(idOrName string) error {
	return ars.DeleteContainer(idOrName)
}

// GetContainerMetadata returns the metadata we've stored for a container.
func (ars *atomfsRuntimeServer) GetContainerMetadata(idOrName string) (RuntimeContainerMetadata, error) {
	md, ok := ars.runtimeMetadata[idOrName]
	if !ok {
		return RuntimeContainerMetadata{}, ErrInvalidContainerID
	}
	return *md, nil
}

// SetContainerMetadata updates the metadata we've stored for a container.
func (ars *atomfsRuntimeServer) SetContainerMetadata(idOrName string, metadata *RuntimeContainerMetadata) error {
	ars.runtimeMetadata[idOrName] = metadata
	return nil
}

func (ars *atomfsRuntimeServer) createContainerOrPodSandbox(systemContext *types.SystemContext, podName, podID, imageName, imageAuthFile, imageID, containerName, containerID, metadataName, uid, namespace string, attempt uint32, idMappings *idtools.IDMappings, labelOptions []string, isPauseImage bool) (ContainerInfo, error) {
	if podName == "" || podID == "" {
		return ContainerInfo{}, ErrInvalidPodName
	}
	if imageName == "" && imageID == "" {
		return ContainerInfo{}, ErrInvalidImageName
	}
	if containerName == "" {
		return ContainerInfo{}, ErrInvalidContainerName
	}
	if metadataName == "" {
		metadataName = containerName
	}

	var oci casext.Engine
	var err error
	ociDir := path.Join(ars.graphRoot, "oci")
        if _, statErr := os.Stat(ociDir); statErr != nil {
                oci, err = umoci.CreateLayout(ociDir)
        } else {
                oci, err = umoci.OpenLayout(ociDir)
        }
	if err != nil {
		return ContainerInfo{}, err
	}
	defer oci.Close()

	err = imageCopy(ImageCopyOpts{
		Src:      imageName,
		Dest:     fmt.Sprintf("oci:%s/oci:%s", ars.graphRoot, containerName),
		Progress: nil,
	})
	if err != nil {
		return ContainerInfo{}, err
	}

	metadata := RuntimeContainerMetadata{
		Pod:           containerID == podID,
		PodName:       podName,
		PodID:         podID,
		ImageName:     imageName,
		ImageID:       imageID,
		ContainerName: containerName,
		MetadataName:  metadataName,
		UID:           uid,
		Namespace:     namespace,
		Attempt:       attempt,
		CreatedAt:     time.Now().Unix(),
	}
	ars.runtimeMetadata[containerID] = &metadata

	containerDir, err := ars.GetWorkDir(containerID)
	if err != nil {
		return ContainerInfo{}, err
	}

	containerRunDir, err := ars.GetRunDir(containerID)
	if err != nil {
		return ContainerInfo{}, err
	}

	manifest, err := lookupManifest(oci, containerName)
	if err != nil {
		return ContainerInfo{}, err
	}

	config, err := lookupConfig(oci, manifest.Config)
	if err != nil {
		return ContainerInfo{}, err
	}

	return ContainerInfo{
		ID:           containerID,
		Dir:          containerDir,
		RunDir:       containerRunDir,
		Config:       &config,
		ProcessLabel: "", // FIXME: selinux labels
		MountLabel:   "",
	}, nil
}

func (ars *atomfsRuntimeServer) CreateContainer(systemContext *types.SystemContext, podName, podID, imageName, imageID, containerName, containerID, metadataName string, attempt uint32, idMappings *idtools.IDMappings, labelOptions []string) (ContainerInfo, error) {
	return ars.createContainerOrPodSandbox(systemContext, podName, podID, imageName, "", imageID, containerName, containerID, metadataName, "", "", attempt, idMappings, labelOptions, false)
}

func (ars *atomfsRuntimeServer) DeleteContainer(idOrName string) error {
	oci, err := umoci.OpenLayout(path.Join(ars.graphRoot, "oci"))
	if err != nil {
		return err
	}
	defer oci.Close()

	err = oci.DeleteReference(ars.ctx, idOrName)
	if err != nil {
		return err
	}

	return oci.GC(ars.ctx)
}

func (ars *atomfsRuntimeServer) StartContainer(idOrName string) (string, error) {
	mountpoint := path.Join(ars.runtimeDir, "rootfses", idOrName)
	metadata := path.Join(ars.runtimeDir, "atomfs-metadata")
	writable := true
	opts := atomfs.MountOCIOpts{
		OCIDir:       path.Join(ars.graphRoot, "oci"),
		MetadataPath: metadata,
		Tag:          idOrName,
		Target:       mountpoint,
		Writable:     writable,
	}

	mol, err := atomfs.BuildMoleculeFromOCI(opts)
	if err != nil {
		return "", err
	}

	return mountpoint, mol.Mount(mountpoint, writable)
}

func (ars *atomfsRuntimeServer) StopContainer(idOrName string) error {
	mountpoint := path.Join(ars.runtimeDir, "rootfses", idOrName)
	metadata := path.Join(ars.runtimeDir, "atomfs-metadata")
	writable := true
	opts := atomfs.MountOCIOpts{
		OCIDir:       path.Join(ars.graphRoot, "oci"),
		MetadataPath: metadata,
		Tag:          idOrName,
		Target:       mountpoint,
		Writable:     writable,
	}

	return atomfs.UnmountOCI(opts)
}

func (ars *atomfsRuntimeServer) GetWorkDir(id string) (string, error) {
	dir := path.Join(ars.graphRoot, "workdir", id)
	return dir, os.MkdirAll(dir, 0755)
}

func (ars *atomfsRuntimeServer) GetRunDir(id string) (string, error) {
	dir := path.Join(ars.runtimeDir, "rundir", id)
	return dir, os.MkdirAll(dir, 0755)
}

// == dumb brute force gitrdun copy paste to avoid dep on anuvu/stacker/lib

var urlSchemes map[string]func(string) (types.ImageReference, error)

func RegisterURLScheme(scheme string, f func(string) (types.ImageReference, error)) {
	urlSchemes[scheme] = f
}

func init() {
	// These should only be things which have pure go dependencies. Things
	// with additional C dependencies (e.g. containers/image/storage)
	// should live in their own package, so people can choose to add those
	// deps or not.
	urlSchemes = map[string]func(string) (types.ImageReference, error){}
	RegisterURLScheme("oci", layout.ParseReference)
	RegisterURLScheme("docker", docker.ParseReference)
	RegisterURLScheme("zot", zot.Transport.ParseReference)
}

func localRefParser(ref string) (types.ImageReference, error) {
	parts := strings.SplitN(ref, ":", 2)
	if len(parts) != 2 {
		return nil, errors.Errorf("bad image ref: %s", ref)
	}

	f, ok := urlSchemes[parts[0]]
	if !ok {
		return nil, errors.Errorf("unknown url scheme %s for %s", parts[0], ref)
	}

	return f(parts[1])
}

type ImageCopyOpts struct {
	Src          string
	Dest         string
	DestUsername string
	DestPassword string
	SkipTLS      bool
	Progress     io.Writer
}

func imageCopy(opts ImageCopyOpts) error {
	srcRef, err := localRefParser(opts.Src)
	if err != nil {
		return err
	}

	destRef, err := localRefParser(opts.Dest)
	if err != nil {
		return err
	}

	// lol. and all this crap is the reason we make everyone install
	// libgpgme-dev, and we don't even want to use it :(
	policy, err := signature.NewPolicyContext(&signature.Policy{
		Default: []signature.PolicyRequirement{
			signature.NewPRInsecureAcceptAnything(),
		},
	})
	if err != nil {
		return err
	}

	args := &copy.Options{
		ReportWriter: opts.Progress,
	}

	if opts.SkipTLS {
		args.SourceCtx = &types.SystemContext{
			DockerInsecureSkipTLSVerify: types.OptionalBoolTrue,
		}
	}

	args.DestinationCtx = &types.SystemContext{
		OCIAcceptUncompressedLayers: true,
	}

	if opts.DestUsername != "" {
		// DoTo check if destination is really a docker URL, maybe it's a zot URL
		args.DestinationCtx.DockerAuthConfig = &types.DockerAuthConfig{
			Username: opts.DestUsername,
			Password: opts.DestPassword,
		}
	}

	_, err = copy.Image(context.Background(), policy, destRef, srcRef, args)
	return err
}

// from oci.go

func lookupManifest(oci casext.Engine, tag string) (ispec.Manifest, error) {
	descriptorPaths, err := oci.ResolveReference(context.Background(), tag)
	if err != nil {
		return ispec.Manifest{}, err
	}

	if len(descriptorPaths) != 1 {
		return ispec.Manifest{}, errors.Errorf("bad descriptor %s", tag)
	}

	blob, err := oci.FromDescriptor(context.Background(), descriptorPaths[0].Descriptor())
	if err != nil {
		return ispec.Manifest{}, err
	}
	defer blob.Close()

	if blob.Descriptor.MediaType != ispec.MediaTypeImageManifest {
		return ispec.Manifest{}, errors.Errorf("descriptor does not point to a manifest: %s", blob.Descriptor.MediaType)
	}

	return blob.Data.(ispec.Manifest), nil
}

func lookupConfig(oci casext.Engine, desc ispec.Descriptor) (ispec.Image, error) {
	configBlob, err := oci.FromDescriptor(context.Background(), desc)
	if err != nil {
		return ispec.Image{}, err
	}

	if configBlob.Descriptor.MediaType != ispec.MediaTypeImageConfig {
		return ispec.Image{}, fmt.Errorf("bad image config type: %s", configBlob.Descriptor.MediaType)
	}

	return configBlob.Data.(ispec.Image), nil

}
