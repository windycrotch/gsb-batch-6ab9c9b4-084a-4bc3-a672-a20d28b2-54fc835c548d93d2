package main

import (
	"flag"
	"net"
	"strconv"

	"github.com/jordicenzano/go-ts-segmenter/manifestgenerator"
	"github.com/jordicenzano/go-ts-segmenter/manifestgenerator/hls"
	"github.com/jordicenzano/go-ts-segmenter/manifestgenerator/mediachunk"
	"github.com/jordicenzano/go-ts-segmenter/uploaders/httpuploader"
	"github.com/jordicenzano/go-ts-segmenter/uploaders/s3uploader"
	"github.com/sirupsen/logrus"

	"bufio"
	"fmt"
	"io"
	"os"
)

const (
	readBufferSize = 128
)

var (
	verbose                 = flag.Bool("verbose", false, "enable to get verbose logging")
	baseOutPath             = flag.String("dstPath", "./results", "Output path")
	chunkBaseFilename       = flag.String("chunksBaseFilename", "chunk_", "Chunks base filename")
	chunkListFilename       = flag.String("chunklistFilename", "chunklist.m3u8", "Chunklist filename")
	targetSegmentDurS       = flag.Float64("targetDur", 4.0, "Target chunk duration in seconds")
	liveWindowSize          = flag.Int("liveWindowSize", 3, "Live window size in chunks")
	lhlsAdvancedChunks      = flag.Int("lhls", 0, "If > 0 activates LHLS, and it indicates the number of advanced chunks to create")
	manifestTypeInt         = flag.Int("manifestType", int(hls.LiveWindow), "Manifest to generate (0- Vod, 1- Live event, 2- Live sliding window")
	autoPID                 = flag.Bool("apids", true, "Enable auto PID detection, if true no need to pass vpid and apid")
	videoPID                = flag.Int("vpid", -1, "Video PID to parse")
	audioPID                = flag.Int("apid", -1, "Audio PID to parse")
	chunkInitType           = flag.Int("initType", int(manifestgenerator.ChunkInitStart), "Indicates where to put the init data PAT and PMT packets (0- No ini data, 1- Init segment, 2- At the beginning of each chunk")
	mediaDestinationType    = flag.Int("mediaDestinationType", 1, "Indicates where the destination (0- No output, 1- File + flag indicator, 2- HTTP chunked transfer, 3- HTTP regular, 4- S3 regular)")
	manifestDestinationType = flag.Int("manifestDestinationType", 1, "Indicates where the destination (0- No output, 1- File + flag indicator, 2- HTTP, 3- S3)")
	httpScheme              = flag.String("protocol", "http", "HTTP Scheme (http, https)")
	httpHost                = flag.String("host", "localhost:9094", "HTTP Host")
	logPath                 = flag.String("logsPath", "", "Logs file path")
	httpMaxRetries          = flag.Int("httpMaxRetries", 40, "Max retries (attempts after the first one) for retryable HTTP errors such as 503. 4xx errors fail immediately and are not retried. On exhaustion the error is propagated, the object is kept out of the HLS manifest and the process exits non-zero")
	initialHTTPRetryDelay   = flag.Int("initialHTTPRetryDelay", 5, "Initial retry delay in MS for chunk HTTP (no chunk transfer) uploads. The delay grows per attempt: attempt N waits (N+1) * initialHttpRetryDelay MS")
	httpsInsecure           = flag.Bool("insecure", false, "Skips CA verification for HTTPS out")
	inputType               = flag.Int("inputType", 1, "Where gets the input data (1-stdin, 2-TCP socket)")
	localPort               = flag.Int("localPort", 2002, "Local port to listen in case inputType = 2")
	awsID                   = flag.String("awsId", "", "AWSId in case you do not want to use default machine credentials")
	awsSecret               = flag.String("awsSecret", "", "AWSSecret in case you do not want to use default machine credentials")
	awsRegion               = flag.String("s3Region", "", "Specific aws region to use for AWS S3 destination")
	s3Bucket                = flag.String("s3Bucket", "", "S3 bucket to upload files, in case of sing an S3 destination")
	s3UploadTimeOut         = flag.Int("s3UploadTimeout", 10000, "Timeout for any S3 upload in MS")
	s3IsPublicRead          = flag.Bool("s3IsPublicRead", false, "Set ACL = \"public-read\" for all S3 uploads")
)

func main() {
	flag.Parse()
	os.Exit(execute(os.Stdin))
}

// execute runs the segmenter reading from inputReader. It returns the process
// exit code: 0 on success, 1 on any fatal error. In particular an upload or
// manifest failure is always propagated here so the process ends with a
// non-zero status and the failed object never reaches the HLS manifest.
func execute(inputReader io.Reader) int {
	var log = configureLogger(*verbose, *logPath)

	log.Info(manifestgenerator.Version, logPath)
	log.Info("Started tssegmenter", logPath)

	if *autoPID == false && manifestgenerator.ChunkInitTypes(*chunkInitType) != manifestgenerator.ChunkNoIni {
		log.Error("Manual PID mode and Chunk No ini data are not compatible")
		return 1
	}

	chunkOutputType := mediachunk.OutputTypes(*mediaDestinationType)
	hlsOutputType := hls.OutputTypes(*manifestDestinationType)

	// Creating output dir if does not exists
	if chunkOutputType == mediachunk.ChunkOutputModeFile || hlsOutputType == hls.HlsOutputModeFile {
		os.MkdirAll(*baseOutPath, 0744)
	}

	var httpUploader *httpuploader.HTTPUploader = nil
	var s3Uploader *s3uploader.S3Uploader = nil
	if isHTTPOut() {
		httpUploaderTmp := httpuploader.New(log, *httpsInsecure, *httpScheme, *httpHost, *httpMaxRetries, *initialHTTPRetryDelay)
		httpUploader = &httpUploaderTmp
	} else if isS3Out() {
		awsCreds := s3uploader.AWSLocalCreds{}
		if (*awsID != "") && (*awsSecret != "") {
			awsCreds.Valid = true
			awsCreds.AWSId = *awsID
			awsCreds.AWSSecret = *awsSecret
		}
		s3UploaderTmp := s3uploader.New(log, *s3Bucket, *awsRegion, *s3UploadTimeOut, *s3IsPublicRead, awsCreds)
		s3Uploader = &s3UploaderTmp
	}

	mg := manifestgenerator.New(log,
		chunkOutputType,
		hlsOutputType,
		*baseOutPath,
		*chunkBaseFilename,
		*chunkListFilename,
		*targetSegmentDurS,
		manifestgenerator.ChunkInitTypes(*chunkInitType),
		*autoPID,
		-1,
		-1,
		hls.ManifestTypes(*manifestTypeInt),
		*liveWindowSize,
		*lhlsAdvancedChunks,
		httpUploader,
		s3Uploader)

	// Create the requested input reader
	var r *bufio.Reader = nil
	if *inputType == 2 {
		// Reader from TCP server socket

		log.Info("Listening on port " + strconv.Itoa(*localPort))
		// listen on all interfaces
		ln, errListen := net.Listen("tcp", ":"+strconv.Itoa(*localPort))
		if errListen != nil {
			log.Error("Error listening on TCP port ", *localPort, ". Err: ", errListen)
			return 1
		}
		// accept connection on port
		conn, errAccept := ln.Accept()
		if errAccept != nil {
			log.Error("Error accepting TCP connection. Err: ", errAccept)
			return 1
		}
		log.Info("Connection TCP accepted")

		r = bufio.NewReader(conn)
	} else {
		// Reader from std in (or the reader passed in, useful for tests)
		r = bufio.NewReader(inputReader)
	}

	// Buffer
	buf := make([]byte, 0, readBufferSize)

	for {
		n, err := r.Read(buf[:cap(buf)])
		if n == 0 && err == io.EOF {
			// Detected EOF
			// Closing
			log.Info("Closing process detected EOF")
			if errClose := mg.Close(); errClose != nil {
				log.Error("Fatal error closing the manifest generator. Err: ", errClose)
				return 1
			}

			break
		}

		if err != nil && err != io.EOF {
			// Error reading pipe
			log.Error(err, logPath)
			return 1
		}

		// process buf
		log.Debug("Sent to process: ", n, " bytes")
		if errAdd := mg.AddData(buf[:n]); errAdd != nil {
			log.Error("Fatal error processing data, stopping so no manifest references an uploaded-failed chunk. Err: ", errAdd)
			return 1
		}
	}

	log.Info("Exit because detected EOF in the input reader")

	return 0
}

func isHTTPOut() bool {
	if (*mediaDestinationType == 2) || (*mediaDestinationType == 3) || (*manifestDestinationType == 2) {
		return true
	}
	return false
}

func isS3Out() bool {
	if (*mediaDestinationType == 4) || (*manifestDestinationType == 3) {
		return true
	}
	return false
}

func configureLogger(verbose bool, logPath string) *logrus.Logger {
	var log = logrus.New()
	if verbose {
		log.SetLevel(logrus.DebugLevel)
	}

	formatter := new(logrus.JSONFormatter)
	formatter.TimestampFormat = "01-01-2001 13:00:00"

	log.SetFormatter(formatter)
	log.SetFormatter(&logrus.JSONFormatter{})

	var mw io.Writer
	if logPath != "" {
		f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			fmt.Printf(fmt.Sprintf("Unable to open log file at: %s, error: %v", logPath, err))
			os.Exit(-1)
		}

		mw = io.MultiWriter(os.Stdout, f)
	} else {
		mw = io.MultiWriter(os.Stdout)
	}

	log.SetOutput(mw)

	return log
}
