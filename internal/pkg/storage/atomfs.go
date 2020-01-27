package storage

import (
	"context"
	"path"
	"time"

	"github.com/anuvu/atomfs"
	"github.com/containers/image/v5/types"
	"github.com/containers/storage/pkg/idtools"
	"github.com/openSUSE/umoci"
)

type atomfsRuntimeServer struct {
	ctx        context.Context
	graphRoot  string
	runtimeDir string
}

func newAtomfsServer(ctx context.Context, imgServer Server) (RuntimeServer, error) {
	return &atomfsRuntimeServer{ctx, imgServer.GetStore().GraphRoot(), imgServer.GetStore().RunRoot()}
}


func (ars *atomfsRunteimServer) CreatePodSandbox(systemContext *types.SystemContext, podName, podID, imageName, imageAuthFile, imageID, containerName, metadataName, uid, namespace string, attempt uint32, idMappings *idtools.IDMappings, labelOptions []string) (ContainerInfo, error) {
	return ars.CreateContainer(...)
}

func (ars *atomfsRuntimeServer) RemovePodSandbox(idOrName string) error {
	return ars.DeleteContainer(idOrName)
}

	// GetContainerMetadata returns the metadata we've stored for a container.
	GetContainerMetadata(idOrName string) (RuntimeContainerMetadata, error)
	// SetContainerMetadata updates the metadata we've stored for a container.
	SetContainerMetadata(idOrName string, metadata *RuntimeContainerMetadata) error

func (ars *atomfsRuntimeServer) CreateContainer(systemContext *types.SystemContext, podName, podID, imageName, imageID, containerName, containerID, metadataName string, attempt uint32, idMappings *idtools.IDMappings, labelOptions []string) (ContainerInfo, error) {
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

	oci, err := umoci.OpenLayout(path.Join(ars.graphRoot, "oci"))
	if err != nil {
		return err
	}

	defer oci.Close()

	manifest, err := lookupManifest(oci, containerName)
	if err != nil {
		return ContainerInfo{}, err
	}

	config, err := lookupConfig(oci, manifest.Config)
	if err != nil {
		return ContainerInfo{}, err
	}

	ci := ContainerInfo{}
	err = imageCopy(stackerlib.ImageCopyOpts{
		Src:      imageName,
		Dest:     fmt.Sprintf("oci:%s/oci:%s", ars.graphRoot, containerName)
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

	containerDir, err := ars.GetWorkDir(containerID)
	if err != nil {
		return ContainerInfo{}, err
	}

	containerRunDir, err := ars.GetRunDir(containerID)
	if err != nil {
		return ContainerInfo{}, err
	}

	return ContainerInfo{
		ID: containerID,
		Dir: ars.GetWorkDir
		RunDir: containerRunDir,
		Config: imageConfig,
		ProcessLabel: "", // FIXME: selinux labels
		MountLabel: "",
	}, nil
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

	return oci.GC()
}

func (ars *atomfsRuntimeServer) StartContainer(idOrName string) (string, error) {
	moutpoint := path.Join(ars.runtimeDir, "rootfses", idOrName)
	metadata := path.Join(ars.runtimeDir, "atomfs-metadata")
	writable := true
	opts := atomfs.MountOCIOpts{
		OCIDir: path.Join(ars.graphRoot, "oci"),
		MetadataPath: metadata,
		Tag: idOrName,
		Target: mountpoint,
		Writable: writable,
	}

	mol, err := atomfs.BuildMoleculeFromOCI(opts)
	if err != nil {
		return "", err
	}

	return mountpoint, mol.Mount(mountpoint, writable)
}

func (ars *atomfsRuntimeServer) StopContainer(idOrName string) error {
	moutpoint := path.Join(ars.runtimeDir, "rootfses", idOrName)
	metadata := path.Join(ars.runtimeDir, "atomfs-metadata")
	writable := true
	opts := atomfs.MountOCIOpts{
		OCIDir: path.Join(ars.graphRoot, "oci"),
		MetadataPath: metadata,
		Tag: idOrName,
		Target: mountpoint,
		Writable: writable,
	}

	return atomfs.UnmountOCI(opts)
}

func (ars *atomfsRuntimeServer) GetWorkDir(id string) (string, error) {
	dir := path.Join(ars.graphRoot, "workdir", id)
	_, err := os.MkdirAll(dir, 0755)
	return dir, err
}

func (ars *atomfsRuntimeServer) GetRunDir(id string) (string, error) {
	dir := path.Join(ars.runtimeDir, "rundir", id)
	_, err := os.MkdirAll(dir, 0755)
	return dir, err
}

// == dumb brute force gitrdun copy paste to avoid dep on anuvu/stacker/lib

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

func ImageCopy(opts ImageCopyOpts) error {
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
