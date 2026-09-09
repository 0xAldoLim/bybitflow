# Bybit host and Docker connectivity audit

Observed 2026-09-09 00:56–00:58 UTC (2026-09-08 local EDT). No TLS trust settings,
DNS resolvers, regional routing, or system clock settings were changed.

| Check | Host | Existing Python 3.12 Docker image |
|---|---|---|
| api.bybit.com DNS | 202.3.218.137 | 202.3.218.137 |
| public.bybit.com DNS | 202.3.218.137 | Not separately tested |
| HTTP(S) proxy environment | None | None |
| Default CA file | /usr/lib/ssl/cert.pem | /usr/lib/ssl/cert.pem |
| Default CA directory | /usr/lib/ssl/certs | /usr/lib/ssl/certs |
| Clock | NTP active, synchronized; RTC matched | UTC matched host |
| SNI | api.bybit.com | api.bybit.com |
| Verified connection | hostname mismatch and expired leaf | Python rejects hostname mismatch |

The presented leaf subject was `CN=internetbaik.telkomsel.com, O=PT Telekomunikasi
Selular, L=Jakarta, C=ID`. Its SANs covered Telkomsel hosts, not Bybit. Validity ended
2023-09-15. Its issuer was DigiCert TLS RSA SHA256 2020 CA1, chaining to DigiCert Global
Root CA; the root/intermediate were recognized by host trust. This is **not** a missing
Bybit CA or Python-only failure. Evidence is consistent with ISP DNS redirection/filtering;
we have not independently established the provider's policy or legal reason.

Do not install this certificate, disable verification, change time, use alternate DNS/IP
overrides, or route around restrictions. Ask the ISP/exchange for supported, lawful access.
Keep `FLOW_SCAN_ENABLED=false` here. TradingView-only proxy research does not contact Bybit.

Reproducible read-only diagnostics:

```sh
date -u
timedatectl status
getent ahostsv4 api.bybit.com
getent ahostsv4 public.bybit.com
timeout 12 openssl s_client -connect api.bybit.com:443 -servername api.bybit.com \
  -verify_hostname api.bybit.com -verify_return_error -brief </dev/null
python -c 'import ssl; print(ssl.get_default_verify_paths())'
docker compose exec desk python -c 'import socket,ssl; h="api.bybit.com"; print(socket.gethostbyname_ex(h)); s=ssl.create_default_context().wrap_socket(socket.create_connection((h,443),timeout=8),server_hostname=h); print(s.version())'
```

Inspect proxy variable **hostnames only**, never dump the environment or credential-bearing URLs.
No actual Bybit historical dataset was acquired during this follow-up.
