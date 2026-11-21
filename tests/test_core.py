import io
import base64
import time
from jobs.csv_processing import process_csv_chunk, combine_csv_results, split_csv
from jobs.image_processing import process_image
from coordinator.scheduler import allocations
from worker.hardware_monitor import metrics
from worker.process_manager import ProcessManager
from PIL import Image

def test_metrics_are_real_shape():
 m=metrics(); assert m['logical_cores'] >= 1 and m['total_ram'] > 0 and 0 <= m['cpu_percent'] <= 100
def test_csv_partition_and_combine():
 data=b'a,b\n1,x\n2,\n3,y\n'; parts=[process_csv_chunk(p) for p in split_csv(data,2)]; result=combine_csv_results(parts)
 assert result['rows']==3 and result['columns']['a']['sum']==6 and result['columns']['a']['mean']==2 and result['missing']['b']==1
def test_image_resize():
 source=io.BytesIO(); Image.new('RGB',(40,30),'red').save(source,format='PNG'); data,fmt=process_image(source.getvalue(),'resize',{'width':10,'height':8,'format':'PNG'})
 assert fmt=='png' and Image.open(io.BytesIO(data)).size==(10,8)
def test_weighted_prefers_idle_machine():
 workers=[{'id':'busy','connected':True,'active_tasks':0,'metrics':{'cpu_percent':45,'ram_percent':60,'available_ram':3_000,'total_ram':10_000,'logical_cores':4}},{'id':'idle','connected':True,'active_tasks':0,'metrics':{'cpu_percent':5,'ram_percent':10,'available_ram':8_000,'total_ram':10_000,'logical_cores':8}}]
 a=allocations(workers,'weighted'); assert a['idle']>a['busy'] and round(sum(a.values()),6)==1

def test_allowlisted_job_runs_in_child_process():
    payload = {
        "job_type": "benchmark",
        "data": base64.b64encode(b"").decode(),
        "params": {"start": 1, "end": 1000},
    }
    result = ProcessManager(timeout=5).run(payload)
    assert result["ok"] is True
    assert result["result"]["value"] >= 0
    assert result["process"]["pid"] > 0
    assert result["process"]["exit_code"] == 0
    assert result["process"]["elapsed_seconds"] >= 0

def test_job_process_timeout_terminates_child():
    payload = {
        "job_type": "benchmark",
        "data": base64.b64encode(b"").decode(),
        "params": {"start": 1, "end": 200_000_000},
    }
    started = time.monotonic()
    result = ProcessManager(timeout=0.05).run(payload)
    elapsed = time.monotonic() - started
    assert result["ok"] is False
    assert result["process"]["timed_out"] is True
    assert elapsed < 3

def test_invalid_allowlisted_job_parameter_returns_error():
    payload = {
        "job_type": "benchmark",
        "data": base64.b64encode(b"").decode(),
        "params": {"start": "invalid", "end": 10},
    }
    result = ProcessManager(timeout=5).run(payload)
    assert result["ok"] is False
    assert result["process"]["exit_code"] == 1
    assert "error" in result

