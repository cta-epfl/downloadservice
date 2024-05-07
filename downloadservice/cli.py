import logging

from downloadservice.app import app

from cheroot.wsgi import PathInfoDispatcher
from cheroot.wsgi import Server

def main():
    logging.basicConfig(level=logging.DEBUG)

    d = PathInfoDispatcher({'/': app})
    server = Server(('0.0.0.0', 5000), d)
    logger = logging.getLogger(__name__)
    logger.info("Serving on http://0.0.0.0:5000")
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
