from datetime import datetime
import OpenSSL
import re


class CertificateError(Exception):
    def __init__(self, message="invalid certificate"):
        self.message = message
        super().__init__(self.message)


def verify_certificate(cabundle, certificate):
    try:
        _PEM_RE = re.compile(
            '-----BEGIN CERTIFICATE-----\r?.+?\r?' +
            '-----END CERTIFICATE-----\r?\n?', re.DOTALL)

        def parse_chain(chain):
            # returns a list of certificates
            return [c.group() for c in _PEM_RE.finditer(chain)]

        client_cert = OpenSSL.crypto.load_certificate(
            OpenSSL.crypto.FILETYPE_PEM, certificate)

        store = OpenSSL.crypto.X509Store()
        store.set_flags(OpenSSL.crypto.X509StoreFlags.ALLOW_PROXY_CERTS)
        for cert in parse_chain(cabundle):
            store.add_cert(OpenSSL.crypto.load_certificate(
                OpenSSL.crypto.FILETYPE_PEM, cert))

        ctx = OpenSSL.crypto.X509StoreContext(store, client_cert)
        ctx.verify_certificate()

        if client_cert.has_expired():
            raise CertificateError("certificate expired")

    except OpenSSL.crypto.X509StoreContextError:
        raise CertificateError('invalid certificate verification chain')
    except OpenSSL.crypto.Error as e:
        raise CertificateError('invalid certificate : '+str(e))


def certificate_validity(certificate):
    try:
        x509 = OpenSSL.crypto.load_certificate(
            OpenSSL.crypto.FILETYPE_PEM, certificate)
        asn1_time = x509.get_notAfter()
        return datetime.strptime(asn1_time.decode(), '%Y%m%d%H%M%S%fZ')
    except OpenSSL.crypto.Error as e:
        raise CertificateError('invalid certificate : '+str(e))
