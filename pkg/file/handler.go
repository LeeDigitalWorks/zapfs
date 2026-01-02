// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"io"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

func (fs *FileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		fs.PostHandler(w, r)
	case http.MethodDelete:
		fs.DeleteHandler(w, r)
	case http.MethodGet:
		fs.GetHandler(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	objectID, err := parseURLPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	obj := &types.ObjectRef{ID: objectID}
	err = fs.store.PutObject(r.Context(), obj, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (fs *FileServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	objectID, err := parseURLPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = fs.store.DeleteObject(r.Context(), objectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (fs *FileServer) GetHandler(w http.ResponseWriter, r *http.Request) {
	objectID, err := parseURLPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	reader, err := fs.store.GetObjectData(r.Context(), objectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	buf := utils.SyncPoolGetBuffer()
	buf.Reset()
	defer utils.SyncPoolPutBuffer(buf)

	w.WriteHeader(http.StatusOK)
	if _, err := io.CopyBuffer(w, reader, buf.Bytes()); err != nil {
		return
	}
}
