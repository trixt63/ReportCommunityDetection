import click

from constants.network_constants import Chains
from utils.logger_utils import get_logger
from src.find_subgraph.gridsearch_subgraph_radius_job import GridSearchSubgraphRadiusJob


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('-e', '--end-block', type=int, help='End block')
@click.option('-s', '--start-block', type=int, help='Start block')
@click.option('-r', '--radius', type=int, help='Subgraph radius')
@click.option('-w', '--max-workers', default=8, show_default=True, type=int, help='The number of workers')
@click.option('-b', '--batch-size', default=1000, show_default=True, type=int, help='Batch size')
def gridsearch_subgraph_radius(chain, end_block, start_block, radius, max_workers, batch_size):
    chain = str(chain).lower()
    if chain not in Chains.mapping:
        raise click.BadOptionUsage("--chain", f"Chain {chain} is not support")
    chain_id = Chains.mapping[chain]

    job = GridSearchSubgraphRadiusJob(
        chain_id=chain_id,
        start_block=start_block,
        end_block=end_block,
        radius=radius,
        batch_size=batch_size,
        max_workers=max_workers
    )
    job.run()
