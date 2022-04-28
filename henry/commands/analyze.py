from concurrent.futures import Future, ThreadPoolExecutor
from typing import Union, cast, Optional, List

from looker_sdk.sdk.api40 import models
from henry.modules import spinner
from henry.modules import fetcher


class Analyze(fetcher.Fetcher):
    @classmethod
    def run(cls, user_input: fetcher.Input):
        analyze = cls(user_input)
        if user_input.subcommand == "projects":
            result = analyze.projects(id=user_input.project)
        elif user_input.subcommand == "models":
            result = analyze.models(project=user_input.project, model=user_input.model)
        elif user_input.subcommand == "explores":
            result = analyze.explores(
                model=user_input.model, explore=user_input.explore
            )
        else:
          raise ValueError(
              "Please specify one of 'projects', 'models' or 'explores'")
        analyze.output(data=cast(fetcher.TResult, result))

    @spinner.Spinner()
    def projects(self, *, id: Optional[str] = None) -> fetcher.TResult:
        """Analyzes all projects or a specific project."""
        projects = self.get_projects(project_id=id)
        result: List[fetcher.TResult] = []
        futures: List[Union[Future, str]] = []
        with ThreadPoolExecutor(max_workers=self.threads) as pool:
            for p in projects:
                assert isinstance(p.name, str)
                assert isinstance(p.pull_request_mode, models.PullRequestMode)
                assert isinstance(p.validation_required, bool)
                p_files = pool.submit(self.sdk.all_project_files, p.name)
                futures.append(p_files)
                if "/bare_models/" in cast(str, p.git_remote_url):
                    git_connection_test_results: Union[Future, str] = "Bare repo, no tests required"
                    git_results_pending = False
                else:
                    git_connection_test_results = pool.submit(
                        self.run_git_connection_tests, cast(str, p.id))
                    git_results_pending = True
                    futures.append(git_connection_test_results)
                result.append(
                    {
                        "Project": p.name,
                        "pending_files": p_files,
                        "Git Connection Status": git_connection_test_results,
                        "git_results_pending": git_results_pending,
                        "PR Mode": p.pull_request_mode.value,
                        "Is Validation Required": p.validation_required,
                    }
                )
            while True:
                if all([f.done() for f in futures]):
                    break
        for r in result:
            if r['git_results_pending']:
                r['Git Connection Status'] = r['Git Connection Status'].result()
            del r['git_results_pending']
            r["# Models"] = sum(
                map(lambda x: x.type == "model", r["pending_files"].result()))
            r["# View Files"] = sum(
                map(lambda x: x.type == "view", r["pending_files"].result()))
            del r['pending_files']
        return result

    @spinner.Spinner()
    def models(
        self, *, project: Optional[str] = None, model: Optional[str] = None
    ) -> fetcher.TResult:
        """Analyze models, can optionally filter by project or model."""
        all_models = self.get_models(project=project, model=model)
        result: fetcher.TResult = []
        futures: List[Future] = []
        with ThreadPoolExecutor(max_workers=self.threads) as pool:
            for m in all_models:
                assert isinstance(m.name, str)
                assert isinstance(m.project_name, str)
                assert isinstance(m.explores, list)
                unused_ex = pool.submit(self.get_unused_explores, model=m.name)
                query_count = pool.submit(self.get_used_models)
                futures.extend([unused_ex, query_count])
                result.append(
                    {
                        "Project": m.project_name,
                        "Model": m.name,
                        "# Explores": len(m.explores),
                        "# Unused Explores": unused_ex,
                        "Query Count": query_count
                    }
                )
            while True:
                if all([f.done() for f in futures]):
                    break
        for r in result:
            r["# Unused Explores"] = len(r["# Unused Explores"].result())
            r["Query Count"] = r["Query Count"].result().get(m.name) or 0
        return result

    @spinner.Spinner()
    def explores(
        self, *, model: Optional[str] = None, explore: Optional[str] = None
    ) -> fetcher.TResult:
        """Analyze explores."""
        all_explores = self.get_explores(model=model, explore=explore)
        result: fetcher.TResult = []
        futures: List[Future] = []
        with ThreadPoolExecutor(max_workers=self.threads) as pool:
            for e in all_explores:
                assert isinstance(e.name, str)
                assert isinstance(e.model_name, str)
                assert isinstance(e.hidden, bool)
                field_stats = self.get_explore_field_stats(e)
                join_stats = pool.submit(self.get_explore_join_stats, explore=e, field_stats=field_stats)
                query_stats = pool.submit(self.get_used_explores, model=e.model_name, explore=e.name)
                futures.extend([join_stats, query_stats])
                result.append(
                    {
                        "Model": e.model_name,
                        "Explore": e.name,
                        "Is Hidden": e.hidden,
                        "Has Description": True if e.description else False,
                        "# Fields": len(field_stats),
                        "# Unused Fields": len(self._filter(field_stats)),
                        "join_stats": join_stats,
                        "query_stats": query_stats
                    }
                )
            while True:
                if all([f.done() for f in futures]):
                    break
        for r in result:
            join_result = r["join_stats"].result()
            query_result = r["query_stats"].result()
            r["# Joins"] = len(join_result)
            r["# Unused Joins"] = len(self._filter(join_result))
            r["Query Count"] = query_result.get(e.name, 0)
            del r['join_stats']
            del r['query_stats']

        return result
