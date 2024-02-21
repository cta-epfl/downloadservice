
listen:
	FLASK_SECRET=$(shell cat secret-env.sh) \
	FLASK_APP=downloadservice \
		     flask run --debug

secret-env.sh:
	openssl rand -hex 64 > secret-env.sh
	chmod 700 secret-env.sh
