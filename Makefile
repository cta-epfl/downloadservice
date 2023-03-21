
listen:
	FLASK_SECRET=$(shell cat secret-env.sh) \
	CTADS_CLIENTCERT=/tmp/x509up_u1000 \
	CTADS_CABUNDLE=/home/savchenk/cabundle.pem \
	FLASK_APP=downloadservice \
		     flask run --debug

secret-env.sh:
	openssl rand -hex 64 > secret-env.sh
	chmod 700 secret-env.sh
