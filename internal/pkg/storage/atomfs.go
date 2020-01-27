package storage

import (
	"path"
	"time"

	"github.com/anuvu/atomfs"
	stackerimage "github.com/anuvu/stacker/lib"
	stackeroci "github.com/anuvu/stacker/oci"
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

	manifest, err := stackeroci.LookupManifest(oci, containerName)
	if err != nil {
		return ContainerInfo{}, err
	}

	config, err := stackeroci.LookupConfig(oci, manifest.Config)
	if err != nil {
		return ContainerInfo{}, err
	}

	ci := ContainerInfo{}
	err := stackerimage.ImageCopy(stackerlib.ImageCopyOpts{
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
