import { existsSync, mkdirSync, readFileSync, chmodSync } from 'node:fs';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { X509Certificate } from 'node:crypto';
export function loadCertificate(directory) {
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const certificate = join(directory, 'dtls-cert.pem'), privateKey = join(directory, 'dtls-key.pem');
  if (!existsSync(certificate) || !existsSync(privateKey)) {
    execFileSync('openssl', ['req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes', '-keyout', privateKey, '-out', certificate, '-days', '3650', '-subj', '/CN=Battlefront Eternal'], { stdio: 'ignore' });
    chmodSync(privateKey, 0o600);
  }
  const cert = new X509Certificate(readFileSync(certificate));
  return { certificate, privateKey, fingerprint: 'sha-256 ' + cert.fingerprint256 };
}
