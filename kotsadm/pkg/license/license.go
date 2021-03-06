package license

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	apptypes "github.com/replicatedhq/kots/kotsadm/pkg/app/types"
	"github.com/replicatedhq/kots/kotsadm/pkg/preflight"
	"github.com/replicatedhq/kots/kotsadm/pkg/render"
	"github.com/replicatedhq/kots/kotsadm/pkg/store"
	"github.com/replicatedhq/kots/kotsadm/pkg/version"
	kotsv1beta1 "github.com/replicatedhq/kots/kotskinds/apis/kots/v1beta1"
	kotslicense "github.com/replicatedhq/kots/pkg/license"
	kotspull "github.com/replicatedhq/kots/pkg/pull"
	"k8s.io/client-go/kubernetes/scheme"
)

func Sync(a *apptypes.App, licenseString string, failOnVersionCreate bool) (*kotsv1beta1.License, error) {
	currentLicense, err := store.GetStore().GetLatestLicenseForApp(a.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get current license")
	}

	var updatedLicense *kotsv1beta1.License
	if licenseString != "" {
		decode := scheme.Codecs.UniversalDeserializer().Decode
		obj, _, err := decode([]byte(licenseString), nil, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse license")
		}

		unverifiedLicense := obj.(*kotsv1beta1.License)
		verifiedLicense, err := kotspull.VerifySignature(unverifiedLicense)
		if err != nil {
			return nil, errors.Wrap(err, "failed to verify license")
		}

		updatedLicense = verifiedLicense
	} else {
		// get from the api
		licenseData, err := kotslicense.GetLatestLicense(currentLicense)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get latest license")
		}
		updatedLicense = licenseData.License
		licenseString = string(licenseData.LicenseBytes)
	}

	// Save and make a new version if the sequence has changed
	if updatedLicense.Spec.LicenseSequence != currentLicense.Spec.LicenseSequence {
		archiveDir, err := ioutil.TempDir("", "kotsadm")
		if err != nil {
			return nil, errors.Wrap(err, "failed to create temp dir")
		}
		defer os.RemoveAll(archiveDir)

		err = store.GetStore().GetAppVersionArchive(a.ID, a.CurrentSequence, archiveDir)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get latest app version")
		}

		newSequence, err := store.GetStore().UpdateAppLicense(a.ID, a.CurrentSequence, archiveDir, updatedLicense, licenseString, failOnVersionCreate, &version.DownstreamGitOps{}, &render.Renderer{})
		if err != nil {
			return nil, errors.Wrap(err, "failed to update license")
		}

		if err := preflight.Run(a.ID, a.Slug, newSequence, a.IsAirgap, archiveDir); err != nil {
			return nil, errors.Wrap(err, "failed to run preflights")
		}
	}

	return updatedLicense, nil
}

// Gets the license as it was at a given app sequence
func GetCurrentLicenseString(a *apptypes.App) (string, error) {
	archiveDir, err := ioutil.TempDir("", "kotsadm")
	if err != nil {
		return "", errors.Wrap(err, "failed to create temp dir")
	}
	defer os.RemoveAll(archiveDir)

	err = store.GetStore().GetAppVersionArchive(a.ID, a.CurrentSequence, archiveDir)
	if err != nil {
		return "", errors.Wrap(err, "failed to get latest app version")
	}

	kotsLicense, err := ioutil.ReadFile(filepath.Join(archiveDir, "upstream", "userdata", "license.yaml"))
	if err != nil {
		return "", errors.Wrap(err, "failed to read license file from archive")
	}
	return string(kotsLicense), nil
}

func CheckDoesLicenseExists(allLicenses []*kotsv1beta1.License, uploadedLicense string) (*kotsv1beta1.License, error) {
	parsedUploadedLicense, err := GetParsedLicense(uploadedLicense)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse uploaded license")
	}

	for _, license := range allLicenses {
		if license.Spec.LicenseID == parsedUploadedLicense.Spec.LicenseID {
			return license, nil
		}
	}
	return nil, nil
}

func GetParsedLicense(licenseStr string) (*kotsv1beta1.License, error) {
	decode := scheme.Codecs.UniversalDeserializer().Decode
	obj, _, err := decode([]byte(licenseStr), nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode license yaml")
	}
	license := obj.(*kotsv1beta1.License)
	return license, nil
}
