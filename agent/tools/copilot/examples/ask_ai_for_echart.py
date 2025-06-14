

from .path_tools import generate_html_path


def get_ask_echart_block_prompt(question):
    graph_type = """
        use pyecharts 2.0. the Python function should only return a string of html. do not save it.
        remind:
        1. No calculation in this step, just draw graph with given data, it has been already selected and processed by previous steps
        2. please choose different graph type based on the question, do not always use bar.
        3. no graph title no set theme! no theme! no theme ! 
        """

    example_code = """
        here is an example: 
        ```python
        def process_data(dataframes_dict):
            import pandas as pd
            import math
            from pyecharts import #
            # do not set theme!!!
            # generate code to perform operations here

            html_string = chart.render_notebook().data # this returns a html string
            return html_string
        ```
        """
    return question + graph_type + example_code



def get_ask_echart_file_prompt(question, tmp_file=False):
    graph_type = """
            use pyecharts 2.0. the Python function should return a string file path in ./tmp_imgs/ only 
            and the graph html generated should be stored in that path. 
            remind:
            1. No calculation in this step, just draw graph with given data, it has been already selected and processed by previous steps
            2. please choose different graph type based on the question, do not always use bar.
            3. no graph title no set theme! no theme! no theme ! 
            file path must be:
            """

    example_code = """
            here is an example: 
            ```python
            def process_data(dataframes_dict):
                import pandas as pd
                import math
                from pyecharts import #
                # generate code to perform operations here
                chart.render(file_path)
                return file_path
            ```
            """
    if not tmp_file:
        return question + graph_type + generate_html_path() + example_code
    else:
        return question + graph_type + "./tmp_imgs/tmp.html" + example_code


