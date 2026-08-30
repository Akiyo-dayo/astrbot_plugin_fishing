import importlib
import sys
import types
import unittest


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def _install_astrbot_stub():
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")
    api.logger = _Logger()
    astrbot.api = api
    sys.modules.setdefault("astrbot", astrbot)
    sys.modules.setdefault("astrbot.api", api)


class _ClosableRepository:
    def __init__(self):
        self.close_count = 0

    def close_connection(self):
        self.close_count += 1


def _raise_loop_error():
    raise RuntimeError("forced loop failure")


class ThreadConnectionLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.achievement_module = importlib.import_module(
            "core.services.achievement_service"
        )
        cls.exchange_module = importlib.import_module(
            "core.services.exchange_price_service"
        )
        cls.fishing_module = importlib.import_module("core.services.fishing_service")

    def test_achievement_thread_closes_every_repository_on_failure(self):
        service = self.achievement_module.AchievementService.__new__(
            self.achievement_module.AchievementService
        )
        repositories = []
        for name in (
            "achievement_repo",
            "user_repo",
            "inventory_repo",
            "item_template_repo",
            "log_repo",
        ):
            repo = _ClosableRepository()
            setattr(service, name, repo)
            repositories.append(repo)
        service._run_achievement_check_loop = _raise_loop_error

        with self.assertRaisesRegex(RuntimeError, "forced loop failure"):
            service._achievement_check_loop()

        self.assertTrue(all(repo.close_count == 1 for repo in repositories))

    def test_exchange_thread_closes_repository_on_failure(self):
        service = self.exchange_module.ExchangePriceService.__new__(
            self.exchange_module.ExchangePriceService
        )
        service.exchange_repo = _ClosableRepository()
        service._run_daily_price_update_loop = _raise_loop_error

        with self.assertRaisesRegex(RuntimeError, "forced loop failure"):
            service._daily_price_update_loop()

        self.assertEqual(service.exchange_repo.close_count, 1)

    def test_fishing_threads_close_every_repository_on_failure(self):
        for loop_name, wrapper_name in (
            ("_run_auto_fishing_loop", "_auto_fishing_loop"),
            ("_run_daily_tax_loop", "_daily_tax_loop"),
        ):
            with self.subTest(loop=wrapper_name):
                service = self.fishing_module.FishingService.__new__(
                    self.fishing_module.FishingService
                )
                repositories = []
                for name in (
                    "user_repo",
                    "inventory_repo",
                    "item_template_repo",
                    "log_repo",
                    "buff_repo",
                    "bank_repo",
                ):
                    repo = _ClosableRepository()
                    setattr(service, name, repo)
                    repositories.append(repo)
                setattr(service, loop_name, _raise_loop_error)

                with self.assertRaisesRegex(RuntimeError, "forced loop failure"):
                    getattr(service, wrapper_name)()

                self.assertTrue(
                    all(repo.close_count == 1 for repo in repositories)
                )


if __name__ == "__main__":
    unittest.main()
