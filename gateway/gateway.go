package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	ufsio "github.com/ipfs/go-unixfs/io"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/libp2p/go-libp2p-core/routing"
)

const (
	IpfsPathPrefix = "/ipfs/"
)

var log = logging.Logger("gateway")

type GatewayConfig struct {
	Headers      map[string][]string
	Writable     bool
	PathPrefixes []string
}

type Peer interface {
	GetFile(ctx context.Context, c cid.Cid) (ufsio.DagReader, error)
}

// gatewayHandler is a HTTP handler that serves IPFS objects (accessible by default at /ipfs/<path>)
// (it serves requests like GET /ipfs/QmVRzPKPzNtSrEzBFm2UZfxmPAgnaLke4DMcerbsGGSaFe/link)
type GatewayHandler struct {
	config GatewayConfig
	peer   Peer
}

// StatusResponseWriter enables us to override HTTP Status Code passed to
// WriteHeader function inside of http.ServeContent.  Decision is based on
// presence of HTTP Headers such as Location.
type statusResponseWriter struct {
	http.ResponseWriter
}

func (sw *statusResponseWriter) WriteHeader(code int) {
	// Check if we need to adjust Status Code to account for scheduled redirect
	// This enables us to return payload along with HTTP 301
	// for subdomain redirect in web browsers while also returning body for cli
	// tools which do not follow redirects by default (curl, wget).
	redirect := sw.ResponseWriter.Header().Get("Location")
	if redirect != "" && code == http.StatusOK {
		code = http.StatusMovedPermanently
	}
	sw.ResponseWriter.WriteHeader(code)
}

func NewGatewayHandler(c GatewayConfig, peer Peer) *GatewayHandler {
	log.Debug("NewGatewayHandler")
	i := &GatewayHandler{
		config: c,
		peer:   peer,
	}
	return i
}

func parseIpfsPath(p string) (cid.Cid, string, error) {
	rootPath, err := path.ParsePath(p)
	if err != nil {
		return cid.Cid{}, "", err
	}

	// Check the path.
	rsegs := rootPath.Segments()
	if rsegs[0] != "ipfs" {
		return cid.Cid{}, "", fmt.Errorf("WritableGateway: only ipfs paths supported")
	}

	rootCid, err := cid.Decode(rsegs[1])
	if err != nil {
		return cid.Cid{}, "", err
	}

	return rootCid, path.Join(rsegs[2:]), nil
}

func (i *GatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Debug("ServeHTTP : ", r.URL.String(), i.config.Writable)

	// the hour is a hard fallback, we don't expect it to happen, but just in case
	ctx, cancel := context.WithTimeout(r.Context(), time.Hour)
	defer cancel()
	r = r.WithContext(ctx)

	defer func() {
		if r := recover(); r != nil {
			log.Error("A panic occurred in the gateway handler!")
			log.Error(r)
			debug.PrintStack()
		}
	}()
	i.getOrHeadHandler(w, r)

	errmsg := "Method " + r.Method + " not allowed: "
	var status int
	if !i.config.Writable {
		status = http.StatusMethodNotAllowed
		errmsg = errmsg + "read only access"
		w.Header().Add("Allow", http.MethodGet)
		w.Header().Add("Allow", http.MethodHead)
		w.Header().Add("Allow", http.MethodOptions)
	} else {
		status = http.StatusBadRequest
		errmsg = errmsg + "bad request for " + r.URL.Path
	}
	http.Error(w, errmsg, status)
}

func (i *GatewayHandler) getOrHeadHandler(w http.ResponseWriter, r *http.Request) {
	urlPath := r.URL.Path
	escapedURLPath := r.URL.EscapedPath()

	// If the gateway is behind a reverse proxy and mounted at a sub-path,
	// the prefix header can be set to signal this sub-path.
	// It will be prepended to links in directory listings and the index.html redirect.
	prefix := ""
	if prfx := r.Header.Get("X-Ipfs-Gateway-Prefix"); len(prfx) > 0 {
		for _, p := range i.config.PathPrefixes {
			if prfx == p || strings.HasPrefix(prfx, p+"/") {
				prefix = prfx
				break
			}
		}
	}
	log.Debug("Prefix ", prefix)
	// HostnameOption might have constructed an IPNS/IPFS path using the Host header.
	// In this case, we need the original path for constructing redirects
	// and links that match the requested URL.
	// For example, http://example.net would become /ipns/example.net, and
	// the redirects and links would end up as http://example.net/ipns/example.net
	requestURI, err := url.ParseRequestURI(r.RequestURI)
	if err != nil {
		webError(w, "failed to parse request path", err, http.StatusInternalServerError)
		return
	}
	originalUrlPath := prefix + requestURI.Path
	log.Debug("originalUrlPath ", originalUrlPath)
	// Service Worker registration request
	if r.Header.Get("Service-Worker") == "script" {
		// Disallow Service Worker registration on namespace roots
		// https://github.com/ipfs/go-ipfs/issues/4025
		matched, _ := regexp.MatchString(`^/ip[fn]s/[^/]+$`, r.URL.Path)
		if matched {
			err := fmt.Errorf("registration is not allowed for this scope")
			webError(w, "navigator.serviceWorker", err, http.StatusBadRequest)
			return
		}
	}

	parsedPath := ipath.New(urlPath)
	log.Debug("parsedPath ", parsedPath)
	if err := parsedPath.IsValid(); err != nil {
		webError(w, "invalid ipfs path", err, http.StatusBadRequest)
		return
	}

	p := strings.Split(urlPath, "/")
	c, err := cid.Decode(p[2])
	switch err {
	case nil:
	default:
		webError(w, "ipfs resolve -r "+escapedURLPath, err, http.StatusNotFound)
		return
	}
	log.Debug("cid ", c.String())
	dr, err := i.peer.GetFile(r.Context(), c)
	if err != nil {
		webError(w, "ipfs cat "+escapedURLPath, err, http.StatusNotFound)
		return
	}
	defer dr.Close()

	// we need to figure out whether this is a directory before doing most of the heavy lifting below
	_, ok := dr.(files.Directory)
	log.Debug("Directory : ", ok)
	responseEtag := `"` + c.String() + `"`
	log.Debug("responseEtag ", responseEtag)

	// Check etag sent back to us
	if r.Header.Get("If-None-Match") == responseEtag || r.Header.Get("If-None-Match") == `W/`+responseEtag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("X-IPFS-Path", urlPath)
	w.Header().Set("Etag", responseEtag)
	modtime := time.Unix(1, 0)

	i.serveFile(w, r, "", modtime, dr)
	// See statusResponseWriter.WriteHeader
	// and https://github.com/ipfs/go-ipfs/issues/7164
	// Note: this needs to occur before listingTemplate.Execute otherwise we get
	// superfluous response.WriteHeader call from prometheus/client_golang
	if w.Header().Get("Location") != "" {
		w.WriteHeader(http.StatusMovedPermanently)
		return
	}

	// A HTML directory index will be presented, be sure to set the correct
	// type instead of relying on autodetection (which may fail).
	w.Header().Set("Content-Type", "text/html")
	if r.Method == http.MethodHead {
		return
	}

	// construct the correct back link
	// https://github.com/ipfs/go-ipfs/issues/1365
	var backLink string = originalUrlPath

	// don't go further up than /ipfs/$hash/
	pathSplit := path.SplitList(urlPath)
	switch {
	// keep backlink
	case len(pathSplit) == 3: // url: /ipfs/$hash

	// keep backlink
	case len(pathSplit) == 4 && pathSplit[3] == "": // url: /ipfs/$hash/

	// add the correct link depending on whether the path ends with a slash
	default:
		if strings.HasSuffix(backLink, "/") {
			backLink += "./.."
		} else {
			backLink += "/.."
		}
	}

	hash := c.String()
	log.Debug(hash)
}

func (i *GatewayHandler) serveFile(w http.ResponseWriter, req *http.Request, name string, modtime time.Time, file ufsio.DagReader) {
	log.Debug("serveFile")
	log.Debug(file.Size())

	w = &statusResponseWriter{w}
	http.ServeContent(w, req, name, modtime, file)
}

func webError(w http.ResponseWriter, message string, err error, defaultCode int) {
	if _, ok := err.(resolver.ErrNoLink); ok {
		webErrorWithCode(w, message, err, http.StatusNotFound)
	} else if err == routing.ErrNotFound {
		webErrorWithCode(w, message, err, http.StatusNotFound)
	} else if err == context.DeadlineExceeded {
		webErrorWithCode(w, message, err, http.StatusRequestTimeout)
	} else {
		webErrorWithCode(w, message, err, defaultCode)
	}
}

func webErrorWithCode(w http.ResponseWriter, message string, err error, code int) {
	http.Error(w, fmt.Sprintf("%s: %s", message, err), code)
	if code >= 500 {
		log.Warnf("server error: %s: %s", err)
	}
}

// return a 500 error and log
func internalWebError(w http.ResponseWriter, err error) {
	webErrorWithCode(w, "internalWebError", err, http.StatusInternalServerError)
}