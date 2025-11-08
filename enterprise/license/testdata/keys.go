//go:build enterprise

// Package testdata contains test keys for unit testing license functionality.
// These keys are for TESTING ONLY - DO NOT use in production!
package testdata

// TestPrivateKeyPEM is an RSA private key for unit tests only.
// DO NOT USE IN PRODUCTION - this key is public!
var TestPrivateKeyPEM = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAu1dB0jrpXj4vustyE2sM4c9M7ltcYlp+DQtm+NiG3Nbvalc2
fMOxzmauWkKVCxPFtXWbi5A8datC9zfjtoUffkz4PpZkbStiAy+FK20iNhhefDZr
XFXp3cwoMl5cT7oBoLi6xaeQzK2Al7Cxy1zfHLgYhyImnGSwZu3bhb4SpPCGj16w
WJWGsyEMdUXNp7u7fBOrpoBn+8gIHtrWB6QtlCMTMIHmYn4oG4HcbL3UGs/mFwnI
MdesLENsFNv9UmaOMvinvl9POkR26sGk4v745Wu+gZWCvAoO+FYjaDGUIhIvRdjv
k8BIkRQcTa+Qm+TuQh67k3YkzXwnxNJ+pKcKXwIDAQABAoIBAAiOnEWPhlBVM9Xq
7AuCBMUdtGxgxPADJSXQYwQz07kroiTv1d/OCoWcgBNEXk1yukGnHRM7Y9+WEol7
Ro30N7OLrX0iXnT0GdZ+aa1Jn1nsNAilbbL6g6Oraxyd3caQfvmJ5pIXrNAEjs6e
9ggNdm135Trq7uuO19XjwHaz72MUO3efiVvIJ7IYl9CUIRgvzEwD6MvRi7w5UnCU
EqfGbuMjZbkU5FWfTjB6mYUzzSgEGQ+XPyyo9/FNQYj8gGC2+ri9BAppSiDc3fsf
ydpB8wWoCkgaK9cr9QRpKdirpkHL2iD9XTixwMBoLg7wX+ClDf3gxAKKOIHOFw7t
cUC5tXkCgYEA2rA7OSWb4No3eD6oWUBgBq6JRTlIbdgwucAekOw51UzVSdH81tDO
bxIhMEZc09/fp8vilsNH8sJl+dYKsqnFrS2D9r7GUWtDocIlKnUwEmMcfw+dN4pA
jQgxZySviaseNDploqJ/P4+zlOE9vbRAxZom1mSoaDBapYJs6YrDkJcCgYEA203X
kcSnrh8DU+N+6OVWGy9FUTLSrPporzp5dbzIDzJexh8OHJETaS5BEsoItwq6Gnzm
DEgxsBGitXwsjTdp+0HE11wZvICKUTRC8TXaa+i2eV8z3MK3O7IgheNXtyGjjoj+
CVW9mQFMIsIN9CuBjgtRqBitaB6DI9hle0KZRXkCgYAmaSPN0sxBPzLU/PRm0MbN
BaT4sqoFGR7d0V/NBqDV1Sv1TlvBg2Vu3tlTeFhfoofPGiGPZ3dgrUJqEm1Imkct
Nrxh/3on3NZBOC98+J5b7GqZ6q0rjy3tV44N1sS6QoMIm2U0nqQJDv103ecRRfLG
gl/l8kIeIrgZMAJfNs9IuwKBgFcrj95PLt+VUDtOGCn2zs0rcwAdlYRQXMAaHCFi
kpsHyIgz2+Ya/H44doFDcUdgqizRLJgBMk7sE6LT4tmXBzdqIxX/c/NrnI8+mMVM
RK/T5oREBVdgxniiCy9s+HbAlkSXy3JmdGCXFW5TeDxv9qVBdom8lWDnj/T1lRDz
nfzxAoGBALgzNwdPronVbwd3asZYq4/Uc2Jf49zk2uuyxMfg4lwfPUxFtkWMcTLM
UMBIgPZL8Cd7mDk9jGAnQiDn6+Fv2b+C4BvcNsiTQxv9Nbl/MWnDcUeTBAnRNuiA
9XufUb29bfIKMeYi29yho5W+uYxH/aplfVoddYGOtfGSqLwl8lsS
-----END RSA PRIVATE KEY-----
`)

// TestPublicKeyPEM is the corresponding RSA public key for unit tests.
// This can be used to verify test licenses - DO NOT USE IN PRODUCTION.
var TestPublicKeyPEM = []byte(`-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu1dB0jrpXj4vustyE2sM
4c9M7ltcYlp+DQtm+NiG3Nbvalc2fMOxzmauWkKVCxPFtXWbi5A8datC9zfjtoUf
fkz4PpZkbStiAy+FK20iNhhefDZrXFXp3cwoMl5cT7oBoLi6xaeQzK2Al7Cxy1zf
HLgYhyImnGSwZu3bhb4SpPCGj16wWJWGsyEMdUXNp7u7fBOrpoBn+8gIHtrWB6Qt
lCMTMIHmYn4oG4HcbL3UGs/mFwnIMdesLENsFNv9UmaOMvinvl9POkR26sGk4v74
5Wu+gZWCvAoO+FYjaDGUIhIvRdjvk8BIkRQcTa+Qm+TuQh67k3YkzXwnxNJ+pKcK
XwIDAQAB
-----END PUBLIC KEY-----
`)
