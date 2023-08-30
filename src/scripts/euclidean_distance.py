import numpy as np
import pandas as pd
from typing import List, Dict


def euclidean_dist(pairs: pd.DataFrame,
                   df_x: pd.DataFrame,
                   df_y: pd.DataFrame,
                   cols_x: List[str],
                   cols_y: List[str]):
    """

    Parameters
    ----------
    pairs
    df_x
    df_y
    cols_x: Must have same length with cols_y
    cols_y: Must have same length with cols_x

    Returns
    -------
    result_df: Dataframe
    """
    try:
        assert len(cols_x) == len(cols_y)
    except AssertionError:
        raise AssertionError("Length cols_x must be similar to cols_y")
        return None
    result_df = pd.DataFrame(data=np.array([[0, 0, 0] for i in pairs.index]), columns=['x', 'y', 'distance'])
    for i in pairs.index:
        x_addr = pairs['x'][i]
        y_addr = pairs['y'][i]
        _distance = 0

        # for col in cols:
        #     _distance += np.square((df_x.loc[x_addr, col] - df_y.loc[y_addr, col]))
        # _distance = np.sqrt(_distance)

        result_df.loc[i, 'x'] = x_addr
        result_df.loc[i, 'y'] = x_addr
        result_df.loc[i, 'distance'] = _distance

    return result_df


def main():
    pairs = pd.read_csv('../../data/0x38_wallets_pairs.csv')
    df_x = pd.read_csv('../../data/')


if __name__ == '__main__':
    main()