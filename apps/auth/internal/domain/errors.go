package domain

import "errors"

var ErrNotFound = errors.New("Not Found")
var ErrUserAlreadyExists = errors.New("User Already Exists")
var ErrInvalidCredentials = errors.New("Invalid Credentials")