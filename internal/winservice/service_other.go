//go:build !windows

package winservice

import "errors"

var errUnsupported = errors.New("windows service management is only available on Windows")

func Run(string, string, string) error     { return errUnsupported }
func Install(string, string, string) error { return errUnsupported }
func Uninstall(string) error               { return errUnsupported }
func Start(string) error                   { return errUnsupported }
func Stop(string) error                    { return errUnsupported }
func Status(string) (string, error)        { return "", errUnsupported }
