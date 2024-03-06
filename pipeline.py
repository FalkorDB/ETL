import subprocess
from datetime import datetime
from falkordb import FalkorDB

class Step:
    def __init__(self, ID, cmd, desc):
        self.ID = ID
        self.cmd = cmd
        self.desc = desc

class Pipeline:
    def __init__(self, name):
        self.name = name
        self.graph = FalkorDB().select_graph(name)

    def steps(self):
        query = "MATCH (s:Step) RETURN s ORDER BY ID(s)"
        result_set = self.graph.query(query).result_set

        steps = []
        for row in result_set:
            steps.append(Step(row.id, row.properties['cmd'], row.properties['desc']))

        return steps

    def create_step(self, cmd, desc):
        query = "CREATE (s:Step {cmd: $cmd, desc: $desc}) RETURN ID(s)"
        params = {'cmd':cmd, 'desc': desc}

        # run query
        res = self.graph.query(query, params)
        ID = res.result_set[0][0]

        # return step ID
        return Step(ID, cmd, desc)

    def connect_steps(self, src, dest):
        query = """MATCH (src), (dest)
                   WHERE ID(src) = $src AND ID(dest) = $dest
                   CREATE (src)-[:NEXT]->(dest)"""
        params = {'src': src.ID, 'dest': dest.ID}

        # run query
        self.graph.query(query, params)

    def clone(self):
        clone = Pipeline(self.name + "_" + str(datetime.now()))
        clone.graph = self.graph.copy(clone.name)
        return clone

    def run_step(self, step):
        # execute step and update step's output and exit code
        cmd = step.cmd
        print(f"Executing {cmd}")

        # Run the shell command
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        exit_code = result.returncode
        output = result.stdout
        print("Output:", output)

        # update step output
        update_query = """MATCH (s:Step)
                          WHERE ID(s) = $id
                          SET s.output = $output, s.exit_code = $exit_code"""
        params = {'id': step.ID, 'output': output, 'exit_code': exit_code}
        self.graph.query(update_query, params)

    def initial_step(self):
        # get first step
        initial_step = "MATCH (s:Step) WHERE indegree(s) = 0 RETURN s"
        s = self.graph.query(initial_step).result_set[0][0]
        return Step(s.id, s.properties['cmd'], s.properties['desc'])

    def next_step(self, step):
        next_step = "MATCH (current:Step)-[:NEXT]->(next:Step) WHERE ID(current) = $current RETURN next"
        params = {'current': step.ID}
        result_set = self.graph.query(next_step, params).result_set

        # check if we've reached the end of our pipeline
        if len(result_set) == 0:
            # no more steps
            return None

        s = result_set[0][0]
        return Step(s.id, s.properties['cmd'], s.properties['desc'])

    def run(self):
        # clone pipeline
        pipeline = self.clone()

        print(f"Running pipeline {pipeline.name}")

        # get first step
        step = pipeline.initial_step()
        pipeline.run_step(step)

        # run step
        while True:
            # get next step
            step = pipeline.next_step(step)
            if step is None:
                break

            pipeline.run_step(step)

        print(f"Finished running pipeline {pipeline.name}")
        return pipeline

def main():
    etl = Pipeline("line_count")

    # Create steps
    A = etl.create_step("curl -o graph.c https://raw.githubusercontent.com/FalkorDB/FalkorDB/master/src/graph/graph.c", "download source")
    B = etl.create_step("wc -l ./graph.c", "count lines")

    # Connect steps
    etl.connect_steps(A, B)

    # Run ETL
    invocation = etl.run()

    # Modify ETL
    # add a new file deletion step
    C = etl.create_step("rm ./graph.c", "delete file")

    # Connect steps
    etl.connect_steps(B, C)

    # Run ETL
    invocation = etl.run()

if __name__ == "__main__":
    main()

