package manifestgenerator

import (
	"fmt"
	"path"

	"github.com/jordicenzano/go-ts-segmenter/manifestgenerator/hls"
	"github.com/jordicenzano/go-ts-segmenter/manifestgenerator/mediachunk"
	"github.com/jordicenzano/go-ts-segmenter/manifestgenerator/tspacket"
	"github.com/jordicenzano/go-ts-segmenter/uploaders/httpuploader"
	"github.com/jordicenzano/go-ts-segmenter/uploaders/s3uploader"
	"github.com/sirupsen/logrus"
)

// Version Indicates the package version
var Version = "1.1.0"

// HlsDefaultVersion to use
const HlsDefaultVersion int = 3

// ChunkInitTypes types indicates where to put the init data (PAT and PMT)
type ChunkInitTypes int

const (
	// ChunkNoIni Necessary if you choose manual PIDs selection
	ChunkNoIni ChunkInitTypes = iota

	//ChunkInit Creates the init segment
	ChunkInit

	//ChunkInitStart Adds PAT and PAT at each chunk start (CC will be broken)
	ChunkInitStart
)

const (
	//GhostPrefixDefault ghost chunk prefix
	GhostPrefixDefault = ".growing_"

	//ChunkFileNumberLength chunk filenumber length
	ChunkFileNumberLength = 5

	//ChunkFileExtensionDefault default chunk extension
	ChunkFileExtensionDefault = ".ts"

	//ChunkInitFileName Init chunk filename
	ChunkInitFileName = "init"
)

const (
	// ChunkLengthToleranceS Tolerance calculating chunk length
	ChunkLengthToleranceS = 0.25
)

// packetTableTypes
type packetTableTypes int

const (
	// PatTable PAT
	PatTable = iota

	// PmtTable PAT
	PmtTable
)

// initState
type initStates int

const (
	// InitNotIni no PAT / PMT saved
	InitNotIni = iota

	// InitsavedPAT PAT saved, needs PMT too
	InitsavedPAT

	// InitsavedPMT PMT and PAT saved
	InitsavedPMT
)

type options struct {
	log                *logrus.Logger
	chunkOutputType    mediachunk.OutputTypes
	manifestOutputType hls.OutputTypes
	baseOutPath        string
	chunkBaseFilename  string
	targetSegmentDurS  float64
	chunkInitType      ChunkInitTypes
	autoPIDs           bool
	videoPID           int
	audioPID           int
	manifestType       hls.ManifestTypes
	liveWindowSize     int
	lhlsAdvancedChunks int
	httpUploader       *httpuploader.HTTPUploader
	s3Uploader         *s3uploader.S3Uploader
}

// ManifestGenerator Creates the manifest and chunks the media
type ManifestGenerator struct {
	options options

	// Internal parsing data
	isInSync        bool
	bytesToNextSync int
	detectedPMTID   int

	// Current TS packet data
	tsPacket tspacket.TsPacket

	// Time counters
	chunkStartTimeS float64
	lastPCRS        float64

	// Packet counter
	processedPackets uint64

	//currentChunks info (1 element array for HLS)
	currentChunks     []mediachunk.Chunk
	currentChunkIndex uint64

	//currentChunk info
	initChunk *mediachunk.Chunk
	initState initStates

	// Packets used to save PAT and PMT (We know we'll break TS CC). Only used in ChunkInitStart mode
	tsInitPATPacket tspacket.TsPacket
	tsInitPMTPacket tspacket.TsPacket

	//Hls generator
	hlsChunklist hls.Hls

	//initialChunkCreation Flag tha indicates the first chunk[s] has been created
	fistChunkCreated bool
}

// New Creates a chunklistgenerator instance
func New(
	log *logrus.Logger,
	chunkOutputType mediachunk.OutputTypes,
	manifestOutputType hls.OutputTypes,
	baseOutPath string,
	chunkBaseFilename string,
	chunkListFilename string,
	targetSegmentDurS float64,
	chunkInitType ChunkInitTypes,
	autoPIDs bool,
	videoPID int,
	audioPID int,
	manifestType hls.ManifestTypes,
	liveWindowSize int,
	lhlsAdvancedChunks int,
	httpUploader *httpuploader.HTTPUploader,
	s3Uploader *s3uploader.S3Uploader,
) ManifestGenerator {
	if log == nil {
		log = logrus.New()
		log.SetLevel(logrus.DebugLevel)
	}

	chunklistFileName := path.Join(baseOutPath, chunkListFilename)

	mg := ManifestGenerator{
		options{
			log,
			chunkOutputType,
			manifestOutputType,
			baseOutPath,
			chunkBaseFilename,
			targetSegmentDurS,
			chunkInitType,
			autoPIDs,
			videoPID,
			audioPID,
			manifestType,
			liveWindowSize,
			lhlsAdvancedChunks,
			httpUploader,
			s3Uploader,
		},
		false,
		0,
		-1,
		tspacket.New(tspacket.TsDefaultPacketSize),
		-1.0,
		-1.0,
		0,
		nil,
		0,
		nil,
		InitNotIni,
		tspacket.New(tspacket.TsDefaultPacketSize),
		tspacket.New(tspacket.TsDefaultPacketSize),
		hls.New(
			log,
			manifestType,
			HlsDefaultVersion,
			true,
			targetSegmentDurS,
			liveWindowSize+lhlsAdvancedChunks,
			chunklistFileName,
			"",
			manifestOutputType,
			httpUploader,
			s3Uploader,
		),
		false,
	}

	return mg
}

func (mg *ManifestGenerator) resync(buf []byte) []byte {
	mg.isInSync = false

	start := 0
	for {
		if start < len(buf) {
			if buf[start] == 0x47 {
				mg.isInSync = true
				break
			} else {
				start++
			}
		} else {
			break
		}
	}

	return buf[start:]
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func (mg *ManifestGenerator) isSavingMediaPacket() bool {
	ret := false
	if !mg.options.autoPIDs {
		// Manual detection PIDs
		ret = true
	} else {
		if mg.options.chunkInitType == ChunkInit || mg.options.chunkInitType == ChunkInitStart {
			if mg.initState == InitsavedPMT {
				ret = true
			}
		} else if mg.options.chunkInitType == ChunkNoIni {
			ret = true
		}
	}

	return ret
}

func (mg *ManifestGenerator) saveInitPacket(tableType packetTableTypes) (bool, error) {
	if mg.options.chunkInitType == ChunkInit {
		return mg.addPacketToInitChunk(tableType)
	} else if mg.options.chunkInitType == ChunkInitStart {
		if tableType == PatTable || tableType == PmtTable {
			return mg.saveInitChunkPacket(tableType), nil
		}

		return false, nil
	}

	return false, nil
}

func (mg *ManifestGenerator) processPacket(forceChunk bool) (bool, error) {
	if !mg.tsPacket.Parse(mg.detectedPMTID) {
		return false, nil
	}

	// Detect video & audio PIDs
	if mg.options.autoPIDs {
		pmtID := mg.tsPacket.GetPATdata()
		if pmtID >= 0 {
			mg.detectedPMTID = pmtID

			// Save PAT
			if _, err := mg.saveInitPacket(PatTable); err != nil {
				return false, err
			}

			mg.options.log.Debug("Detected PAT. PMT ID: ", pmtID)
		}

		valid, Videoh264, AudioADTS, Other := mg.tsPacket.GetPMTdata()
		if valid {
			if len(Videoh264) > 0 {
				mg.options.videoPID = int(Videoh264[0])
			}
			if len(AudioADTS) > 0 {
				mg.options.audioPID = int(AudioADTS[0])
			}

			// Save PMT
			if _, err := mg.saveInitPacket(PmtTable); err != nil {
				return false, err
			}

			mg.options.log.Debug("Detected PMT. VideoIDs: ", Videoh264, "AudiosIDs: ", AudioADTS, "Other: ", Other)
		}
	}

	pID := mg.tsPacket.GetPID()
	if pID == mg.options.videoPID {
		if mg.isSavingMediaPacket() {
			// Detect if we need to chunk it
			// It will chunk if detect an IDR point with PCR data
			if mg.tsPacket.IsRandomAccess(mg.options.videoPID) == true {
				mg.options.log.Debug("VIDEO: ", mg.tsPacket.String())
				pcrS := mg.tsPacket.GetPCRS()
				if pcrS >= 0 {
					mg.lastPCRS = pcrS

					if mg.chunkStartTimeS < 0 && pcrS >= 0 {
						mg.chunkStartTimeS = pcrS
					}
					durS := pcrS - mg.chunkStartTimeS
					if (durS + ChunkLengthToleranceS) > mg.options.targetSegmentDurS {
						var err error
						_, nextInitialPCRS, err := mg.nextChunk(pcrS, mg.chunkStartTimeS, tspacket.MaxPCRSValue, false)
						if err != nil {
							return false, err
						}

						mg.chunkStartTimeS = nextInitialPCRS
					}
				}
			}
			if err := mg.addPacketToChunk(); err != nil {
				return false, err
			}

		} else {
			mg.options.log.Debug("SKIPPED VIDEO PACKET, not init: ", mg.tsPacket.String())
		}
	} else if pID == mg.options.audioPID {
		if mg.isSavingMediaPacket() {
			if err := mg.addPacketToChunk(); err != nil {
				return false, err
			}
			mg.options.log.Debug("AUDIO: ", mg.tsPacket.String())
		} else {
			mg.options.log.Debug("SKIPPED AUDIO PACKET, not init: ", mg.tsPacket.String())
		}
	} else if pID >= 0 {
		mg.options.log.Debug("OTHER: ", mg.tsPacket.String())
	} else {
		fmt.Println("OUT OF SYNC!!!")
		return false, nil
	}

	return true, nil
}

func (mg *ManifestGenerator) addPacketToChunk() error {

	if mg.currentChunks == nil {
		if err := mg.createChunk(false); err != nil {
			return err
		}
	}

	if len(mg.currentChunks) > 0 {

		//In case we need to save PAT and PMT do it just before the 1st packet
		if mg.options.chunkInitType == ChunkInitStart && mg.currentChunks[0].IsEmpty() {
			// Save PAT and PMT first if available
			if mg.initState == InitsavedPMT {
				mg.currentChunks[0].AddData(mg.tsInitPATPacket.GetBuffer())
				mg.currentChunks[0].AddData(mg.tsInitPMTPacket.GetBuffer())
			}
		}

		err := mg.currentChunks[0].AddData(mg.tsPacket.GetBuffer())
		if err != nil {
			return err
		}
	}

	return nil
}

func (mg *ManifestGenerator) saveInitChunkPacket(tableType packetTableTypes) bool {
	ret := false

	if tableType == PatTable {
		if mg.initState == InitNotIni {
			// Save PAT
			mg.tsInitPATPacket = tspacket.CloneFrom(mg.tsPacket)
			mg.initState = InitsavedPAT
			ret = true
		}
	} else if tableType == PmtTable {
		if mg.initState == InitsavedPAT {
			// Save PMT
			mg.tsInitPMTPacket = tspacket.CloneFrom(mg.tsPacket)
			mg.initState = InitsavedPMT
			ret = true
		}
	}

	return ret
}

func (mg *ManifestGenerator) addPacketToInitChunk(tableType packetTableTypes) (bool, error) {
	ret := false
	saveData := false

	if tableType == PatTable {
		if mg.initState == InitNotIni { // We only save the 1st PAT PMT appeareance, so no dynamic updates are allowed
			if mg.initChunk == nil {
				// Create init chunk
				if err := mg.createChunk(true); err != nil {
					return false, err
				}
			}
			saveData = true
		}
	} else if tableType == PmtTable {
		if mg.initState == InitsavedPAT {
			saveData = true
		}
	}

	if saveData {
		err := mg.initChunk.AddData(mg.tsPacket.GetBuffer())
		if err != nil {
			return false, err
		}

		if tableType == PatTable {
			mg.initState = InitsavedPAT
		} else if tableType == PmtTable {
			mg.initState = InitsavedPMT

			if err := mg.closeChunk(true, -1, false); err != nil {
				return false, err
			}
		}

		ret = true
	}

	return ret, nil
}

func (mg *ManifestGenerator) hlsClose() error {
	return mg.hlsChunklist.CloseManifest(true)
}

func (mg *ManifestGenerator) hlsAddChunk(isGrowing bool, fileName string, durationS float64, isDisco bool) error {
	err := mg.hlsChunklist.AddChunk(hls.Chunk{IsGrowing: isGrowing, FileName: fileName, DurationS: durationS, IsDisco: isDisco}, true)
	if err != nil {
		mg.options.log.Error("Error generating / saving the chunklists. Err: ", err)
	}
	return err
}

func (mg *ManifestGenerator) closeChunk(isInit bool, chunkDurationS float64, isFinalChunk bool) error {
	// Close current

	if isInit == false {
		if mg.currentChunks != nil && len(mg.currentChunks) > 0 {
			currentChunk := mg.currentChunks[0]

			// Upload the chunk first. Only when the upload succeeds may the
			// chunk be referenced by the manifest, otherwise the error is
			// propagated and the chunk never enters the chunklist.
			if err := currentChunk.Close(chunkDurationS); err != nil {
				mg.options.log.Error("Error uploading chunk ", currentChunk.GetFilename(), ". Err: ", err)
				return fmt.Errorf("error uploading chunk %s: %w", currentChunk.GetFilename(), err)
			}

			//NO LHLS
			if mg.options.lhlsAdvancedChunks <= 0 {
				if err := mg.hlsAddChunk(false, currentChunk.GetFilename(), chunkDurationS, false); err != nil {
					return err
				}
				if mg.options.manifestType == hls.Vod {
					if isFinalChunk {
						if err := mg.hlsClose(); err != nil {
							return err
						}
					}
				}
			}

			if len(mg.currentChunks) > 1 {
				// Remove 1st element
				mg.currentChunks = mg.currentChunks[1:]
			} else {
				// Empty array
				mg.currentChunks = mg.currentChunks[:0]
			}

			mg.currentChunkIndex++
		}
	} else {
		if mg.initChunk != nil {
			if err := mg.initChunk.Close(-1); err != nil {
				mg.options.log.Error("Error uploading init chunk ", mg.initChunk.GetFilename(), ". Err: ", err)
				return fmt.Errorf("error uploading init chunk %s: %w", mg.initChunk.GetFilename(), err)
			}

			mg.hlsChunklist.SetInitChunk(mg.initChunk.GetFilename())

			// We need to update version 7 for map chunks
			mg.hlsChunklist.SetHlsVersion(7)

			mg.initChunk = nil
		}
	}

	return nil
}

func (mg *ManifestGenerator) createChunk(isInit bool) error {
	// Close current
	if isInit {
		chunkInitOptions := mediachunk.Options{
			Log:                mg.options.log,
			OutputType:         mg.options.chunkOutputType,
			LHLS:               false,
			EstimatedDurationS: -1,
			FileNumberLength:   ChunkFileNumberLength,
			GhostPrefix:        GhostPrefixDefault,
			FileExtension:      ChunkFileExtensionDefault,
			BasePath:           mg.options.baseOutPath,
			ChunkBaseFilename:  ChunkInitFileName,
			HTTPUploader:       mg.options.httpUploader,
			S3Uploader:         mg.options.s3Uploader,
		}

		newChunk := mediachunk.New(0, chunkInitOptions)
		mg.initChunk = &newChunk

		return mg.initChunk.InitializeChunk()
	}

	chunksToCreate := 1
	if mg.fistChunkCreated == false && mg.options.lhlsAdvancedChunks > 0 {
		chunksToCreate = mg.options.lhlsAdvancedChunks
		mg.fistChunkCreated = true
	}

	n := 0
	for n < chunksToCreate {
		chunkOptions := mediachunk.Options{
			Log:                mg.options.log,
			OutputType:         mg.options.chunkOutputType,
			LHLS:               false,
			EstimatedDurationS: mg.options.targetSegmentDurS,
			FileNumberLength:   ChunkFileNumberLength,
			GhostPrefix:        GhostPrefixDefault,
			FileExtension:      ChunkFileExtensionDefault,
			BasePath:           mg.options.baseOutPath,
			ChunkBaseFilename:  mg.options.chunkBaseFilename,
			HTTPUploader:       mg.options.httpUploader,
			S3Uploader:         mg.options.s3Uploader}

		if mg.options.lhlsAdvancedChunks > 0 {
			chunkOptions.LHLS = true
		}

		newChunk := mediachunk.New(mg.currentChunkIndex+uint64(len(mg.currentChunks)), chunkOptions)

		if err := newChunk.InitializeChunk(); err != nil {
			return err
		}

		// Add the advanced chunk to the manifest with target dur
		if mg.options.lhlsAdvancedChunks > 0 {
			if err := mg.hlsAddChunk(true, newChunk.GetFilename(), mg.options.targetSegmentDurS, false); err != nil {
				return err
			}
		}

		mg.currentChunks = append(mg.currentChunks, newChunk)

		n++
	}
	return nil
}

// Creates chunk and returns the initial time for the next chunk
func (mg *ManifestGenerator) nextChunk(currentPCRS float64, lastInitialPCRS float64, maxPCRs float64, isFinalChunk bool) (chunkDurationS float64, nextInitialPCRS float64, err error) {
	chunkDurationS = -1.0
	nextInitialPCRS = currentPCRS

	if currentPCRS >= lastInitialPCRS {
		chunkDurationS = currentPCRS - lastInitialPCRS
	} else {
		// Detected possible PCR roll over
		mg.options.log.Info("Possible PCR rollover! lastInitialPCRS:", lastInitialPCRS, ", currentPCRS: ", currentPCRS, ", maxPCRs: ", maxPCRs)
		chunkDurationS = maxPCRs - currentPCRS + lastInitialPCRS
	}

	mg.options.log.Info("CHUNK! At PCRs: ", currentPCRS, ". ChunkDurS: ", chunkDurationS)

	if err = mg.closeChunk(false, chunkDurationS, isFinalChunk); err != nil {
		return
	}
	if !isFinalChunk {
		err = mg.createChunk(false)
	}

	return
}

// Close Closes manigest processing saving last data and last chunk. It returns
// an error if the final chunk or the final manifest could not be uploaded.
func (mg *ManifestGenerator) Close() error {
	//Generate last chunk
	_, _, err := mg.nextChunk(mg.lastPCRS, mg.chunkStartTimeS, tspacket.MaxPCRSValue, true)
	return err
}

// AddData current chunk
func (mg *ManifestGenerator) AddData(buf []byte) error {
	if !mg.isInSync {
		buf = mg.resync(buf)

		if len(buf) > 0 {
			mg.bytesToNextSync = tspacket.TsDefaultPacketSize
		}
	}

	if len(buf) > 0 {
		addedSize := min(len(buf), mg.bytesToNextSync)
		mg.tsPacket.AddData(buf[:addedSize])

		mg.bytesToNextSync = mg.bytesToNextSync - addedSize

		buf = buf[addedSize:]
	}

	if mg.bytesToNextSync <= 0 {
		// Process packet
		ok, err := mg.processPacket(false)
		if err != nil {
			return err
		}
		if !ok {
			mg.isInSync = false
		} else {
			mg.bytesToNextSync = tspacket.TsDefaultPacketSize
			mg.processedPackets++
			mg.tsPacket.Reset()
		}
	}

	if len(buf) > 0 {
		// Still data to process
		if err := mg.AddData(buf[:]); err != nil {
			return err
		}
	}

	return nil
}

func (mg ManifestGenerator) getNumProcessedPackets() uint64 {
	return mg.processedPackets
}
