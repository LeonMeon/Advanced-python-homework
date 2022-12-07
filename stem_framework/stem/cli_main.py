import argparse
import json
from pathlib import Path
import sys

from stem.task_master import TaskMaster, TaskStatus
from stem.workspace import IWorkspace, TaskPath
    
def print_structure(args: argparse.Namespace):

    def pretty(d, indent=0):
        for key, value in d.items():
            print('\t' * indent + str(key))
            if isinstance(value, dict):
                pretty(value, indent + 1)
            else:
                print('\t' * (indent + 1) + str(value))
                
    pretty(workspace.structure()) # made it wrong

def run_task(workspace: IWorkspace, args: argparse.Namespace): 
    try:
        task = workspace.find_task(TaskPath(args.TASKPATH))
    except:
        raise ValueError("smth with task")
        
    if args.meta is None:
        info = {}
    else:
        try:
            info = json.loads(args.meta)
        except json.JSONDecodeError:
            with open(args.meta) as f:
                info = json.load(f)
        except:
            raise ValueError("smth with meta")
    
    try:
        task_results = TaskMaster().execute(meta, task, workspace)
    except:
        raise ValueError("smth with TaskMaster")
    
    if task_results.status == TaskStatus.CONTAINS_DATA:
        print(task_results.lazy_data())
    else:
        print(task_results)   
    

def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run task in workspace')
    parser.add_argument("-w", "--workspace", metavar="WORKSPACE", required=True,
                        help="Add path to workspace or file for module workspace")
    
    subparser = parser.add_subparsers(metavar="command", required=True)    
    parser_str = subparser.add_parser("structure", help="Print workspace structure")
    parser_str.set_defaults(func=print_structure)

    parser_run = subparser.add_parser("run", help="Run task")
    parser_run.set_defaults(func=run_task)
    parser_run.add_argument("sds", metavar='TASKPATH')
    
    parser_run.add_argument("-m", "--meta", metavar="META", required=True,
                            help="Metadata for task or path to file with metadata in JSON format")
    

    return parser
        

def stem_cli_main():
    parser = create_parser()
    args = parser.parse_args()
    args.func(args)        
        
if __name__ == "__main__":
    stem_cli_main()