import subprocess

def query_experiment_sdfrdfizer():
  config_file = './example/rdfizer/example/config.ini'
  result = subprocess.run(
    ["python", "-m", "rdfizer", "-c", config_file],
    capture_output=True, text=True 
    )

  print("STDOUT:", result.stdout)
  print("STDERR:", result.stderr)
  print("SDM-rdfizer")
  return 

if __name__ == "__main__":
    query_experiment_sdfrdfizer()