# Hello
This is a demo application which accompany the FalkorDB ETL blog post
The main idea is to use FalkorDB as a backend to run state machines

The short source code shows how Falkor can store, query and update different
state machines, the key idea is define a "template" pipeline as a DAG
each invocation of the pipeline creates is executed against a dedicated clone
of the DAG, this way we get a clear separation between different runs,
in addition to the flexibility of modifying existing templates.

# Run

Install requirements:

```sh
pip install -r requirements.txt
```

Run FalkorDB via docker:

```sh
docker run -p 6379:6379 -p 3000:3000 -it --rm falkordb/falkordb:latest
```

Execute demo:

```sh
python pipeline.py
```
