package snapshot

import (
	"bufio"
	"bytes"

	"github.com/pkg/errors"
	"gopkg.in/ini.v1"
)

func FormatAWSCredentials(accessKeyID, secretAccessKey string) ([]byte, error) {
	awsCfg := ini.Empty()
	section, err := awsCfg.NewSection("default")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create default section in aws creds")
	}
	_, err = section.NewKey("aws_access_key_id", accessKeyID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create access key")
	}

	_, err = section.NewKey("aws_secret_access_key", secretAccessKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create secret access key")
	}

	var awsCredentials bytes.Buffer
	writer := bufio.NewWriter(&awsCredentials)
	_, err = awsCfg.WriteTo(writer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to write ini")
	}
	if err := writer.Flush(); err != nil {
		return nil, errors.Wrap(err, "failed to flush buffer")
	}

	return awsCredentials.Bytes(), nil
}
