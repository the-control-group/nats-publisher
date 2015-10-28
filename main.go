package main

import (
	"bufio"
	"bytes"
	_ "encoding/json"
	"errors"
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/apcera/nats"
	"io"
	"net"
	"net/textproto"
	"regexp"
	"time"
)

var publishCmd = regexp.MustCompile(`^publish ([a-zA-z0-9-_\.:]+) ([^\r\n]*)[\r\n]+$`)

var gnatsdHost = flag.String("gnatsd-host", "localhost", "Gnatsd host")
var gnatsdPort = flag.String("gnatsd-port", "4222", "Gnatsd port")

var nc *nats.Conn

func main() {
	flag.Parse()
	nc = newGnatsConnection(*gnatsdHost, *gnatsdPort)
	listenTCP("", "0")
}

func listenTCP(host, port string) (err error) {
	var listener *net.TCPListener
	// Automatically assign open port
	address, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(host, port))
	if err != nil {
		log.WithField("error", err).Fatal("Unable to resolve tcp address")
		return
	}
	listener, err = net.ListenTCP("tcp", address)
	if err != nil {
		log.WithField("error", err).Fatal("Unable to bind to tcp on localhost")
		return
	}
	log.WithField("address", listener.Addr()).Info("Listening")
	defer listener.Close()
	serve(listener)
	return
}

func serve(listener *net.TCPListener) (err error) {
	for {
		select {
		default:
			listener.SetDeadline(time.Now().Add(1e9 * time.Nanosecond))
			var c *net.TCPConn
			c, err = listener.AcceptTCP()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
				log.WithField("error", err).Warn("AcceptTCP")
				return
			}
			log.WithField("client", c.RemoteAddr()).Debug("Connected")
			go func(c *net.TCPConn) {
				handleConnection(c)
			}(c)
		}
	}
}

func handleConnection(c *net.TCPConn) {
	defer c.Close()
	defer log.WithField("client", c.RemoteAddr()).Debug("Disconnected")
	// var timeout = 60 * time.Second
	var lastMsg []byte
	var bufc = bufio.NewReader(c)
	for {
		var msg []byte
		var err error
		select {
		default:
			writer := textproto.NewWriter(bufio.NewWriter(c))
			// c.SetDeadline(time.Now().Add(timeout))
			msg, err = bufc.ReadBytes('\n')
			if err != nil {
				switch err.(type) {
				case net.Error:
					if err.(net.Error).Timeout() {
						log.Warn("Connection timed out")
					}
				default:
					if err != io.EOF {
						log.WithField("error", err).Error("Reading bytes")
					}
				}
				return
			}
			if bytes.Compare(bytes.TrimSpace(msg), []byte(`!!`)) == 0 {
				log.WithField("msg", bytes.NewBuffer(lastMsg).String()).Info("Writing last message")
				c.Write(lastMsg)
				if err != nil {
					log.WithField("err", err).Error("Unable to write last message")
				}
				msg = lastMsg
			}
			switch {
			case bytes.HasPrefix(msg, []byte(`publish `)):
				matches := publishCmd.FindSubmatch(msg)
				if len(matches) != 3 {
					err = errors.New(`Unrecognized pattern for command "publish"`)
				} else {
					//					var event map[string]interface{}
					//					err := json.Unmarshal(matches[2], &event)
					//					if err != nil {
					//						log.WithFields(log.Fields{"err": err, "msg": string(matches[2])}).Error("Bad json")
					//						break
					//					}
					err = nc.Publish(string(matches[1]), matches[2])
					if err != nil {
						log.WithField("err", err).Error("Publish failed")
						break
					}
					log.Debug("Message published")
				}
			case bytes.HasPrefix(msg, []byte(`#`)):
				// Comment line
				msg = lastMsg
			case len(bytes.TrimSpace(msg)) == 0:
				// Empty line
				msg = lastMsg
			default:
				err = errors.New("Unrecognized command")
			}
			if err != nil {
				log.Error(err)
				writer.PrintfLine(err.Error())
				writer.PrintfLine(apiHelp())
			}
			lastMsg = msg
		}
	}
}

func apiHelp() string {
	return `I only understand, "publish <subject> <message>"`
}

func newGnatsConnection(host, port string) *nats.Conn {
	// Setup options to include all servers in the cluster
	opts := nats.DefaultOptions
	opts.Servers = []string{"nats://" + net.JoinHostPort(host, port)}

	log.WithFields(log.Fields{"servers": opts.Servers}).Debug("Found Gnatsd servers")

	opts.MaxReconnect = 5
	opts.ReconnectWait = (2 * time.Second)
	opts.NoRandomize = true

	log.Debug("Connecting to Gnatsd")

	nc, err := opts.Connect()

	log.Debug("Connected to Gnatsd")

	if err != nil {
		log.WithField("error", err).Fatal("Error connecting to Gnatsd cluster: ", err)
	}

	// Setup callbacks to be notified on disconnects and reconnects
	nc.Opts.DisconnectedCB = func(nc *nats.Conn) {
		log.Warn("Gnatsd server disconnected!")
	}

	// See who we are connected to on reconnect.
	nc.Opts.ReconnectedCB = func(nc *nats.Conn) {
		log.WithField("url", nc.ConnectedUrl()).Warn("Gnatsd server reconnected")
	}

	nc.Opts.ClosedCB = func(nc *nats.Conn) {
		log.WithField("url", nc.ConnectedUrl()).Warn("Gnatsd server connection closed")
	}

	nc.Opts.AsyncErrorCB = func(nc *nats.Conn, s *nats.Subscription, err error) {
		log.WithFields(log.Fields{"url": nc.ConnectedUrl(), "subject": s.Subject, "queue": s.Queue, "err": err}).Warn("Gnatsd async error")
	}

	if err != nil {
		log.WithField("error", err).Fatal("Unable to establish encoded connection")
	}

	return nc
}
