package kb

type pdfCloser interface{ Close() error }

func closePDFReaderIfSupported(reader any) {
	if closer, ok := reader.(pdfCloser); ok {
		_ = closer.Close()
	}
}
