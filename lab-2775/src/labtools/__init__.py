"""Laptop-side helpers shared by ``scripts/``, the inspector and the two lab
front-ends (``setup.sh``, ``pipeline.sh``).

Nothing here imports pyflink, streamlit or docling: this is the code that has
to work the same whether it is called from a shell script, from a Streamlit
page or from a test, which is exactly why it is a package under ``src/``
rather than a module beside the scripts that grew it.

    config    lab.yaml -> the environment every component already reads
    kafka     one client configuration for every laptop-side Kafka client
    records   how a chunk record is printed, on a terminal and in the UI
"""
