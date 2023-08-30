import click

from cli.gridsearch_subgraph_radius import gridsearch_subgraph_radius


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass


cli.add_command(gridsearch_subgraph_radius, "gridsearch_subgraph_radius")
