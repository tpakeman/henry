from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from textwrap import fill
from typing import Sequence, cast

from looker_sdk import models
from pytest import fail

from henry.modules import exceptions, fetcher, spinner


class Pulse(fetcher.Fetcher):
    """Runs a number of checks against a given Looker instance to determine
    overall health.
    """

    @classmethod
    @spinner.Spinner()
    def run(cls, user_input: fetcher.Input):
        pulse = cls(user_input)
        tests = [func for func in dir(pulse) if callable(
            getattr(pulse, func)) and func.startswith("check_")]
        with ThreadPoolExecutor(max_workers=user_input.threads) as pool:
            results = [pool.submit(getattr(pulse, t)) for t in tests]
            for i, finished in enumerate(as_completed(results)):
                result = finished.result()
                print(f"\bTest {i + 1}/{len(tests)}: {result['name']}")
                pulse._tabularize_and_print(result['result'])

    def check_db_connections(self):
        """Gets all db connections and runs all supported tests against them.
        """
        reserved_names = ["looker__internal__analytics",
                          "looker", "looker__ilooker"]
        db_connections: Sequence[models.DBConnection] = list(
            filter(lambda c: c.name not in reserved_names,
                   self.sdk.all_connections())
        )

        if not db_connections:
            raise exceptions.NotFoundError("No connections found.")

        formatted_results = []
        with ThreadPoolExecutor(max_workers=self.threads) as pool:
            pending_results = []
            for connection in db_connections:
                assert connection.dialect
                assert isinstance(connection.name, str)
                conn_test = pool.submit(self.sdk.test_connection, connection.name, models.DelimSequence(
                    connection.dialect.connection_tests))
                queries = pool.submit(self.sdk.run_inline_query,
                                      "json",
                                      models.WriteQuery(
                                          model="i__looker",
                                          view="history",
                                          fields=["history.query_run_count"],
                                          filters={
                                              "history.connection_name": connection.name},
                                          limit="1",
                                      ),
                                      )
                pending_results.extend((conn_test, queries))
                formatted_results.append({
                    "Connection": connection.name,
                    "Status": conn_test,
                    "Query Count": queries,
                })

                while True:
                    if all([r.done() for r in pending_results]):
                        break
        for result in formatted_results:
            conn_results = list(filter(lambda r: r.status
                                == "error", result['Status'].result()))
            conn_errors = [
                f"- {fill(cast(str, e.message), width=100)}" for e in conn_results]
            result['Status'] = "OK" if not conn_errors else "\n".join(
                conn_errors)
            query_run_count = json.loads(result['Query Count'].result())[
                0]["history.query_run_count"]
            result['Query Count'] = query_run_count

        return {
            "name": "Checking connections",
            "result": formatted_results
        }

    def check_dashboard_performance(self):
        """Prints a list of dashboards with slow running queries in the past
        7 days"""
        request = models.WriteQuery(
            model="i__looker",
            view="history",
            fields=["dashboard.title, query.count"],
            filters={
                "history.created_date": "7 days",
                "history.real_dash_id": "-NULL",
                "history.runtime": ">30",
                "history.status": "complete",
            },
            sorts=["query.count desc"],
            limit=20,
        )
        resp = self.sdk.run_inline_query("json", request)
        slowest_dashboards = json.loads(resp)
        return {
            "name": "Checking for dashboards with queries slower than 30 seconds in the last 7 days",
            "result": slowest_dashboards
        }

    def check_dashboard_errors(self):
        """Prints a list of erroring dashboard queries."""
        request = models.WriteQuery(
            model="i__looker",
            view="history",
            fields=["dashboard.title", "history.query_run_count"],
            filters={
                "dashboard.title": "-NULL",
                "history.created_date": "7 days",
                "history.dashboard_session": "-NULL",
                "history.status": "error",
            },
            sorts=["history.query_run_ount desc"],
            limit=20,
        )
        resp = self.sdk.run_inline_query("json", request)
        erroring_dashboards = json.loads(resp)
        return {
            "name": "Checking for dashboards with erroring queries in the last 7 days",
            "result": erroring_dashboards
        }

    def check_explore_performance(self):
        """Prints a list of the slowest running explores."""
        request = models.WriteQuery(
            model="i__looker",
            view="history",
            fields=["query.model", "query.view", "history.average_runtime"],
            filters={
                "history.created_date": "7 days",
                "query.model": "-NULL, -system^_^_activity",
            },
            sorts=["history.average_runtime desc"],
            limit=20,
        )
        resp = self.sdk.run_inline_query("json", request)
        slowest_explores = json.loads(resp)

        request.fields = ["history.average_runtime"]
        resp = json.loads(self.sdk.run_inline_query("json", request))
        avg_query_runtime = resp[0]["history.average_runtime"]
        to_print = "Checking for the slowest explores in the past 7 days"
        if avg_query_runtime:
            to_print += f"\b\nFor context, the average query runtime is {avg_query_runtime:.4f}s"
        return {
            "name": to_print,
            "result": slowest_explores
        }

    def check_schedule_failures(self):
        """Prints a list of schedules that have failed in the past 7 days."""
        request = models.WriteQuery(
            model="i__looker",
            view="scheduled_plan",
            fields=["scheduled_job.name", "scheduled_job.count"],
            filters={
                "scheduled_job.created_date": "7 days",
                "scheduled_job.status": "failure",
            },
            sorts=["scheduled_job.count desc"],
            limit=500,
        )
        result = self.sdk.run_inline_query("json", request)
        failed_schedules = json.loads(result)
        return {
            "name": "Checking for failing schedules",
            "result": failed_schedules
        }

    def check_legacy_features(self):
        """Prints a list of enabled legacy features."""
        lf = list(filter(lambda f: f.enabled, self.sdk.all_legacy_features()))
        legacy_features = [{"Feature": cast(str, f.name)} for f in lf]
        return {
            "name": "Checking for enabled legacy features",
            "result": legacy_features
        }
