import os
import sys

pkg_path = os.path.dirname(os.path.dirname(__file__))
sys.path.append(pkg_path)

from fkcookiecutter.celery_helper.app import celery_app

# DEV
# os.environ.setdefault("APP_ENV", "DEV")
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")

# PROD
# os.environ.setdefault("APP_ENV", "PROD")
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")


if __name__ == '__main__':
    # app.worker_main()

    # 本地测试用 -c=1
    # The support for this usage was removed in Celery 5.0. Instead you should use `-A` as a global option:
    # celery -A celeryapp worker < ... >
    # Usage: celery worker [OPTIONS]
    # Try 'celery worker --help' for help.

    """
    /bin/bash -c "source /home/.virtualenv/.setenv_fosun_circle.sh && 
    /home/.virtualenv/fosun_circle_running/bin/celery -A config.celery worker -l info -P gevent --concurrency=50 -n worker1@%%h"
    """
    celery_app.worker_main(argv=["-A", "fkcookiecutter.celery_helper.app", "worker", '-P', 'threads', "-l", "info", "-c", "200"])

    # OK
    # worker = celery_app.Worker(
    #     concurrency=1,
    #     loglevel='INFO',
    #     pool='threads',
    # )
    # worker.start()
