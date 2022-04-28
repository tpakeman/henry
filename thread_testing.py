import argparse
import json

from henry.commands import analyze, pulse, vacuum
from henry.modules import fetcher
from henry.cli import setup_cli
from datetime import datetime as dt


def time(fn):
    def inner(*args, **kwargs):
        s = dt.now()
        r = fn(*args, **kwargs)
        e = dt.now()
        t = (e - s).total_seconds()
        print(
            f'{fn.__name__} completed in {t:.2f} seconds with {args[0]} thread(s)')
        return r
    return inner


def custom_init(parser: argparse.ArgumentParser, threads, quiet=False):
    args = dict(vars(parser.parse_args()))
    args['threads'] = threads
    if quiet:
        args['quiet'] = True
    return fetcher.Input(**args)


@time
def thread_test(n):
    parser = setup_cli()
    user_input = custom_init(parser, n)
    if user_input.command == "pulse":
        pulse.Pulse.run(user_input)
    elif user_input.command == "analyze":
        analyze.Analyze.run(user_input)
    elif user_input.command == "vacuum":
        vacuum.Vacuum.run(user_input)
    else:
        parser.error()


def test_threading(vals):
    for v in vals:
        thread_test(v)


def test_iter(ns, num_iters=8):
    parser = setup_cli()
    results = {}
    n = 0
    total = len(ns) * num_iters
    for _ in range(num_iters):
        for threads in ns:
            results[threads] = {'runs': [], 'avg': None}
            user_input = custom_init(parser, threads, True)
            try:
                n += 1
                s = dt.now()
                if user_input.command == "pulse":
                    pulse.Pulse.run(user_input)
                elif user_input.command == "analyze":
                    analyze.Analyze.run(user_input)
                elif user_input.command == "vacuum":
                    vacuum.Vacuum.run(user_input)
                else:
                    parser.error()
                e = dt.now()
                t = (e - s).total_seconds()
                results[threads]['runs'].append(round(t, 2))
                print(
                    f'Test {n:>3}/{total}: threads: {threads:<2} - time: {t:<5.2f}s')
            except Exception as e:
                print(
                    f"Test {n:>3}/{total}: threads: {threads:<2} - ERROR:\n\t{str(e)}")
                pass
    for k in results.keys():
        if len(results[k]['runs']) > 0:
            results[k]['avg'] = round(
                sum(results[k]['runs']) / len(results[k]['runs']), 2)
    with open('thread_testing_results.json', 'w') as f:
        f.write(json.dumps(results))
    print(results)
    return results


if __name__ == "__main__":
    # test_iter([1, 2, 4, 8], 4)
    test_threading([8, 1])
