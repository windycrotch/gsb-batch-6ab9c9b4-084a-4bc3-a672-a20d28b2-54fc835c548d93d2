package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pion/webrtc/v3"
	"gopkg.in/yaml.v3"
)

var (
	version         string
	buildTime       string
	config          ConfigStruct
	videoTrack      *webrtc.TrackLocalStaticRTP
	audioTrack      *webrtc.TrackLocalStaticRTP
	iceServersArray []string
)

type IceServersStruct struct {
	IceServer string `yaml:"ice-server"`
}

type ConfigStruct struct {
	ServeIp          string             `yaml:"serve-ip"`
	ServePort        string             `yaml:"serve-port"`
	RtpHost          string             `yaml:"rtp-host"`
	EnableVideo      bool               `yaml:"enable-video"`
	EnableAudio      bool               `yaml:"enable-audio"`
	RtpVideoPort     int                `yaml:"rtp-video-port"`
	RtpAudioPort     int                `yaml:"rtp-audio-port"`
	VideoTrackName   string             `yaml:"video-track-name"`
	AudioTrackName   string             `yaml:"audio-track-name"`
	VideoCodec       string             `yaml:"video-codec"`
	AudioCodec       string             `yaml:"audio-codec"`
	IceServers       []IceServersStruct `yaml:"ice-servers"`
	ApiLocation      string             `yaml:"api-location"`
	AllowCrossOrigin bool               `yaml:"allow-cross-origin"`
}

func main() {
	log.Printf("Starting rtp2webrtc %s build on %s", version, buildTime)

	configFilePath := os.Getenv("RTP2WEBRTC_CONFIG_FILE_PATH")
	log.Printf("Loading configuration file located at %s", configFilePath)
	configFile, err := os.ReadFile(configFilePath)
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		log.Fatal(err.Error())
	}

	for _, iceServer := range config.IceServers {
		iceServersArray = append(iceServersArray, iceServer.IceServer)
	}

	if config.EnableVideo {
		videoTrack = initWebrtcTrack("video", config.VideoCodec, config.VideoTrackName)
		go listenRtp(config.RtpHost, config.RtpVideoPort, "video", videoTrack)
	}

	if config.EnableAudio {
		audioTrack = initWebrtcTrack("audio", config.AudioCodec, config.AudioTrackName)
		go listenRtp(config.RtpHost, config.RtpAudioPort, "audio", audioTrack)
	}

	http.HandleFunc(config.ApiLocation, OnConnect)
	log.Printf("Serving API on %s:%s", config.ServeIp, config.ServePort)

	httpServer := &http.Server{
		Addr:    config.ServeIp + ":" + config.ServePort,
		Handler: nil, // use http.DefaultServeMux, where OnConnect was registered
	}

	serverErrors := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	stopSignals := make(chan os.Signal, 1)
	signal.Notify(stopSignals, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		log.Fatalf("HTTP server failed: %v", err)
	case sig := <-stopSignals:
		log.Printf("Received signal %s, shutting down", sig)
	}

	// Stop accepting new offers and bound the wait for in-flight responses.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Graceful HTTP shutdown failed: %v", err)
	}
	shutdownCancel()

	// Close every PeerConnection so all shared-track bindings/subscriptions
	// are removed before the process exits.
	closeAllViewers()

	log.Println("Shutdown complete")
}
