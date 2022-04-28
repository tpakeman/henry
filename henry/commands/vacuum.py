from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, List, cast, Optional

from henry.modules import fetcher
from henry.modules import spinner


class Vacuum(fetcher.Fetcher):
    @classmethod
    def run(cls, user_input: fetcher.Input):
        vacuum = cls(user_input)
        if user_input.subcommand == "models":
            result = vacuum.models(
                project=user_input.project, model=user_input.model)
        elif user_input.subcommand == "explores":
            result = vacuum.explores(
                model=user_input.model, explore=user_input.explore)
        vacuum.output(data=cast(fetcher.TResult, result))

    @spinner.Spinner()
    def models(self, *, project: Optional[str] = None, model: Optional[str]) -> fetcher.TResult:
        """Analyze models."""
        result: fetcher.TResult = []
        futures: List[Any] = []
        with ThreadPoolExecutor(max_workers=self.threads) as pool:
            all_models = pool.submit(
                self.get_models, project=project, model=model)
            used_models = pool.submit(self.get_used_models)
            for m in all_models.result():
                assert isinstance(m.name, str)
                unused_explores = pool.submit(self.get_unused_explores, m.name)
                futures.append(unused_explores)
                result.append(
                    {
                        "Model": m.name,
                        "Model Query Count": used_models.result().get(m.name, 0),
                        "future": unused_explores
                    }
                )
            while True:
                if all([f.done() for f in futures]):
                    break
            for r in result:
                r["Unused Explores"] = "\n".join(sorted(r['future'].result()))
                del r['future']
        return result

    @spinner.Spinner()
    def explores(
        self, *, model: Optional[str] = None, explore: Optional[str] = None
    ) -> fetcher.TResult:
        """Analyze explores"""
        explores = self.get_explores(model=model, explore=explore)
        result: fetcher.TResult = []
        futures: List[Future] = []
        with ThreadPoolExecutor(max_workers=self.threads) as pool:
            for e in explores:
                assert isinstance(e.name, str)
                assert isinstance(e.model_name, str)
                field_stats = self.get_explore_field_stats(e)
                join_stats = pool.submit(
                    self.get_explore_join_stats, explore=e, field_stats=field_stats)
                futures.append(join_stats)
                result.append(
                    {
                        "Model": e.model_name,
                        "Explore": e.name,
                        "Unused Joins": join_stats,
                        "Unused Fields": "\n".join(sorted(self._filter(field_stats))),
                    }
                )
            while True:
                if all([f.done() for f in futures]):
                    break

        for r in result:
            r["Unused Joins"] = "\n".join(
                sorted(self._filter(r["Unused Joins"].result()).keys()))
        return result
