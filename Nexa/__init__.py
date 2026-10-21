import os
from flask import Flask

from config import SECRET_KEY, UPLOAD_FOLDER, MAX_CONTENT_LENGTH, BACKGROUND_VIDEO


def create_app():
    app = Flask(__name__)
    app.secret_key = SECRET_KEY
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)

    from Nexa.admin_routes import admin_bp
    from Nexa.auth_routes import auth_bp
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp, url_prefix='/admin')

    @app.context_processor
    def _globals():
        return {"bg_video": BACKGROUND_VIDEO}

    return app