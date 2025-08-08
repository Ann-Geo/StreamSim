package helpers

import (
	"bytes"
	"encoding/json"
)

func GeneratePlaintext(size int64, element string) []byte {
	switch element {
	case "variable":
		return []byte(RandomString(int(size)))
	case "file":
		return []byte(RandomFileContent(int(size)))
	case "image":
		return []byte(RandomImageText(int(size)))
	default:
		return []byte(RandomString(int(size)))
	}
}
func GenerateJSON(size int64, element string) ([]byte, error) {
	var sample map[string]interface{}

	switch element {
	case "variable":
		sample = map[string]interface{}{
			"var": RandomString(int(size) / 4),
		}
	case "file":
		sample = map[string]interface{}{
			"filename": RandomString(10),
			"content":  RandomString(int(size) / 2),
		}
	case "image":
		sample = map[string]interface{}{
			"image_id": RandomString(8),
			"pixels":   RandomBinaryData(int(size) / 2),
		}
	default:
		sample = map[string]interface{}{
			"data": RandomString(int(size) / 2),
		}
	}

	data, err := json.Marshal(sample)
	if err != nil {
		return nil, err
	}

	// pad if needed
	if int64(len(data)) < size {
		padding := bytes.Repeat([]byte{' '}, int(size)-len(data))
		data = append(data, padding...)
	}
	return data, nil
}

func GenerateBinary(size int64, element string) []byte {
	switch element {
	case "variable", "file", "image":
		return RandomBinaryData(int(size))
	default:
		return RandomBinaryData(int(size))
	}
}

func GenerateHDF5(size int64, element string) []byte {
	header := []byte("\x89HDF\r\n\x1a\n") // HDF5 standard signature
	contentSize := size
	if size > int64(len(header)) {
		contentSize = size - int64(len(header))
	}
	content := RandomBinaryData(int(contentSize))

	switch element {
	case "variable":
		// simulate simple datasets
		dataset := []byte("Dataset: VarData\n")
		return append(header, append(dataset, content...)...)
	case "file":
		dataset := []byte("Dataset: FileContent\n")
		return append(header, append(dataset, content...)...)
	case "image":
		dataset := []byte("Dataset: ImagePixels\n")
		return append(header, append(dataset, content...)...)
	default:
		return append(header, content...)
	}
}
