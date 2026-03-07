"""ログ共通ユーティリティ（aws-lambda-powertools）。"""

from aws_lambda_powertools import Logger


def setup_logger(service: str = None) -> Logger:
    """Lambda用Powertoolsロガーを初期化して返す。

    Args:
        service: サービス名（Noneの場合は環境変数 POWERTOOLS_SERVICE_NAME を使用）

    Returns:
        設定済みPowertools Logger
    """
    return Logger(service=service) if service else Logger()
