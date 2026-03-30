package common

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type (
	fileReader struct {
		data  []Data
		index atomic.Int64
	}

	csvReaderImpl struct {
		defaultSource string
		config        CsvReaderConfig
		readers       map[string]*fileReader
		mutex         sync.Mutex
	}
)

func NewCsvReader(defaultSource string, config CsvReaderConfig) ICsvReader {
	return &csvReaderImpl{
		defaultSource: defaultSource,
		config:        config,
		readers:       make(map[string]*fileReader),
	}
}

func (r *csvReaderImpl) GetData(sourceFile string) (Data, error) {
	key := sourceFile
	if key == "" {
		key = r.defaultSource
	}

	reader := r.getOrCreateReader(key)
	if reader == nil {
		return nil, fmt.Errorf("failed to load csv file: %s", key)
	}

	idx := int(reader.index.Add(1) - 1)
	data := reader.data
	if len(data) == 0 {
		return nil, fmt.Errorf("no data in csv file: %s", key)
	}
	if idx >= len(data) {
		idx = idx % len(data)
	}
	return data[idx], nil
}

func (r *csvReaderImpl) getOrCreateReader(key string) *fileReader {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if reader, ok := r.readers[key]; ok {
		return reader
	}

	data, err := r.readCsvFile(key)
	if err != nil {
		return nil
	}

	reader := &fileReader{
		data:  data,
		index: atomic.Int64{},
	}
	reader.index.Store(0)
	r.readers[key] = reader
	return reader
}

func (r *csvReaderImpl) readCsvFile(sourceFile string) ([]Data, error) {
	file, err := os.Open(sourceFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	comma := []rune(r.config.Delimiter)
	if len(comma) > 0 {
		reader.Comma = comma[0]
	}

	if r.config.WithHeader {
		_, err := reader.Read()
		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	var lines []Data
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		lines = append(lines, row)
		if r.config.Limit > 0 && len(lines) >= r.config.Limit {
			break
		}
	}
	return lines, nil
}

func (r *csvReaderImpl) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	clear(r.readers)
	r.readers = nil
	return nil
}

type (
	CSVWriter struct {
		Path      string
		Header    []string
		Delimiter string
		DataCh    <-chan []string
	}
)

func NewCsvWriter(path, delimiter string, header []string, dataCh <-chan []string) *CSVWriter {
	return &CSVWriter{
		Path:      path,
		Delimiter: delimiter,
		Header:    header,
		DataCh:    dataCh,
	}
}

func (c *CSVWriter) WriteForever() error {
	file, err := os.OpenFile(c.Path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}
	w := csv.NewWriter(file)
	comma := []rune(c.Delimiter)
	if len(comma) > 0 {
		w.Comma = comma[0]
	}
	if err := w.Write(c.Header); err != nil {
		return err
	}
	w.Flush()
	go func() {
		file, err := os.OpenFile(c.Path, os.O_APPEND|os.O_RDWR, 0644)
		defer func() {
			_ = file.Close()
		}()
		if err != nil {
			return
		}
		w := csv.NewWriter(file)
		comma := []rune(c.Delimiter)
		if len(comma) > 0 {
			w.Comma = comma[0]
		}

		for {
			_ = w.Write(<-c.DataCh)
			w.Flush()
		}

	}()
	return nil
}
