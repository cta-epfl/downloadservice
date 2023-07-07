# Download service

Backend server for [https://github.com/cta-epfl/ctadata](https://github.com/cta-epfl/ctadata).

## Installation

For use as a JupyterHub service, run:
```
poetry install
```

For standalone service without JupyterHub :
```
poetry install --with dev --without jupyterhub
Note, however, that in this mode the service will not check authorization and will by default fail to respond to any request for restricted data. However, if `CTADS_DISABLE_ALL_AUTH` application config is set, the service will authorize all requests: this way this mode can also be used for for testing. 
