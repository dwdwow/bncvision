package bncvision

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
)

type DataVisionXML struct {
	XMLName        xml.Name                      `xml:"ListBucketResult"`
	Xmlns          string                        `xml:"xmlns,attr"`
	Name           string                        `xml:"Name"`
	Prefix         string                        `xml:"Prefix"`
	Maker          string                        `xml:"Maker"`
	NextMarker     string                        `xml:"NextMarker"`
	MaxKeys        int64                         `xml:"MaxKeys"`
	Delimiter      string                        `xml:"Delimiter"`
	IsTruncated    bool                          `xml:"IsTruncated"`
	CommonPrefixes []DataVisionXMLCommonPrefixes `xml:"CommonPrefixes"`
	Contents       []DataVisionXMLContent        `xml:"Contents"`
}

type DataVisionXMLCommonPrefixes struct {
	Prefix string `xml:"Prefix"`
}

type DataVisionXMLContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

func QueryDataVisionXML(prefix, marker string) (xmls []DataVisionXML, prefixes []DataVisionXMLCommonPrefixes, contents []DataVisionXMLContent, err error) {
	prefix = strings.Trim(prefix, "/") + "/"
	url := "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=" + prefix
	if marker != "" {
		url += "&marker=" + marker
	}
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	defer func() {
		err = resp.Body.Close()
	}()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	x := DataVisionXML{}
	err = xml.Unmarshal(data, &x)
	if err != nil {
		return
	}
	xmls = append(xmls, x)
	prefixes = append(prefixes, x.CommonPrefixes...)
	contents = append(contents, x.Contents...)

	nextMarker := x.NextMarker
	if nextMarker == "" {
		return
	}

	nextMarker = strings.ReplaceAll(nextMarker, "/", "%2F")

	_xmls, _prefixs, _contents, err := QueryDataVisionXML(prefix, nextMarker)

	xmls = append(xmls, _xmls...)
	prefixes = append(prefixes, _prefixs...)
	contents = append(contents, _contents...)

	return
}

func DownloadWithXMLContents(contents []DataVisionXMLContent, localParentDir string, maxDownloadingNum int8) (undownloadContents []DataVisionXMLContent, err error) {
	wg := errgroup.Group{}
	wg.SetLimit(int(maxDownloadingNum))
	mu := sync.Mutex{}
	for _, content := range contents {
		fileRelativePath := content.Key
		content := content
		wg.Go(func() error {
			fileUrl := DATA_VISION_URL + "/" + fileRelativePath
			gLogger.Info("prepare to download file", "url", fileUrl)
			fileLocation := localParentDir + "/" + fileRelativePath
			_, err := os.Stat(fileLocation)
			if err == nil {
				slog.Info("file exists", "file", fileLocation)
				return nil
			}
			err = DownloadSaveZipWithRetryAndValidate(fileUrl, localParentDir, 3)
			if err != nil {
				gLogger.Error("downloading file", "err", err)
				mu.Lock()
				undownloadContents = append(undownloadContents, content)
				mu.Unlock()
				return err
			}
			slog.Info("file downloaded", "file", fileLocation)
			return nil
		})
	}
	err = wg.Wait()
	return
}
