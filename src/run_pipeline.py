import sys
import os
import argparse
sys.path.append(os.getcwd())
from pathlib import Path
from settings import Settings
import src.task1.run as run_task1
import src.task2.run as run_task2
import src.task3.run as run_task3
import src.task4.run as run_task4
import src.task5.run as run_task5
import src.task6.run as run_task6
from src.util import build_logger

logger = build_logger()

def parse_args():
    """
    解析命令行参数。
    """
    parser = argparse.ArgumentParser(description="Run the pipeline with specified parameters.")
    parser.add_argument("--input_dir", required=True, help="Input directory path.")
    parser.add_argument("--character_json", required=True, help="Path to the character.json file.")
    parser.add_argument("--output_dir", required=True, help="Output directory path.")
    return parser.parse_args()

def resolve_path(path: str) -> Path:
    """
    如果路径是绝对路径，则直接返回。
    如果是相对路径，则基于 Settings.CKG4J_ROOT 拼接完整路径。
    """
    resolved_path = Path(path)
    if not resolved_path.is_absolute():
        resolved_path = Settings.CKG4J_ROOT / resolved_path
    return resolved_path.resolve()

def run_pipeline(input_dir: Path, character_json: Path, output_dir: Path):
    # Resolve directories
    input_dir = resolve_path(input_dir)
    character_json = resolve_path(character_json)
    output_dir = resolve_path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Input Directory: {input_dir}")
    print(f"Character JSON: {character_json}")
    print(f"Output Directory: {output_dir}")
    

    # Task 1

    task1_output_dir = str(Path(output_dir / "task1"))
    if Settings.task_settings.task1["enable"]:
        os.makedirs(task1_output_dir, exist_ok=True)
        try:
            logger.info(f"Running Task1...")
            run_task1.run(
                character_json,
                input_dir,
                task1_output_dir,
                **Settings.task_settings.task1["args"]
            )
            logger.info(f"Task1 finished successfully, output_dir: {task1_output_dir}")
        except Exception as e:
            logger.error(f"Error running Task1: {e}")
            return
    
    # Task 2
    task2_output_dir = str(Path(output_dir / "task2"))
    if Settings.task_settings.task2["enable"]:
        os.makedirs(task2_output_dir, exist_ok=True)
        try:
            logger.info(f"Running Task2...")
            run_task2.run(
                task1_output_dir, 
                task2_output_dir, 
                **Settings.task_settings.task2["args"]
            )
            logger.info(f"Task2 finished successfully, output_dir: {task2_output_dir}")
        except Exception as e:
            logger.error(f"Error running Task2: {e}")
            return
    
    # Task 3
    task3_output_dir = str(Path(output_dir / "task3"))
    if Settings.task_settings.task3["enable"]:
        os.makedirs(task3_output_dir, exist_ok=True)
        try:
            logger.info(f"Running Task3...")
            run_task3.run(
                task2_output_dir, 
                task3_output_dir, 
                **Settings.task_settings.task3["args"]
            )
            logger.info(f"Task3 finished successfully, output_dir: {task3_output_dir}")
        except Exception as e:
            logger.error(f"Error running Task3: {e}")
            return
    
    # Task 4
    task4_output_dir = str(Path(output_dir / "task4"))
    if Settings.task_settings.task4["enable"]:
        os.makedirs(task4_output_dir, exist_ok=True)
        try:
            logger.info(f"Running Task4...")
            run_task4.run(
                task3_output_dir, 
                task4_output_dir, 
                **Settings.task_settings.task4["args"]
            )
            logger.info(f"Task4 finished successfully, output_dir: {task4_output_dir}")
        except Exception as e:
            logger.error(f"Error running Task4: {e}")
            return
    
    # Task 5
    task5_output_dir = Path(output_dir / "task5")
    if Settings.task_settings.task5["enable"]:
        os.makedirs(task5_output_dir, exist_ok=True)
        try:
            logger.info(f"Running Task5...")
            run_task5.run(
                task3_output_dir, 
                task5_output_dir, 
                **Settings.task_settings.task5["args"]
            )
            logger.info(f"Task5 finished successfully, output_dir: {task5_output_dir}")
        except Exception as e:
            logger.error(f"Error running Task5: {e}")
            return
    
    # Task 6 (conditional input based on graph_source)
    task6_output_dir = str(Path(output_dir / "task6"))
    if Settings.task_settings.task6["enable"]:
        graph_source = Settings.task_settings.task6["graph_source"]
        
        if graph_source == 3:
            task6_input_dir = Path(output_dir / "task3")
        elif graph_source == 5:
            task6_input_dir = Path(output_dir / "task5")
        else:
            print(f"Invalid graph_source {graph_source}. Choose either 3 or 5.")
            sys.exit(1)
        
        os.makedirs(task6_output_dir, exist_ok=True)
        try:
            logger.info(f"Running Task6...")
            run_task6.run(
                task4_output_dir,
                task6_input_dir,
                task6_output_dir,
                **Settings.task_settings.task6["args"]
            )
            logger.info(f"Task6 finished successfully, output_dir: {task6_output_dir}")
        except Exception as e:
            logger.error(f"Error running Task6: {e}")
            return

if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.input_dir, args.character_json, args.output_dir)
