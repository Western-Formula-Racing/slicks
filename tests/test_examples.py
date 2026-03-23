import unittest
import sys
import os
import importlib.util

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

def _influx_available() -> bool:
    """Return True only if the CI test time window has wide-schema data."""
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
    try:
        from slicks.fetcher import get_influx_client
        from slicks import config
        client = get_influx_client()
        result = client.query(
            query=(
                f'SELECT "INV_Motor_Speed" FROM "{config.INFLUX_DB}" '
                f"WHERE time >= TIMESTAMP '2025-09-28T20:20:00Z' "
                f"  AND time <= TIMESTAMP '2025-09-28T21:00:00Z' "
                f'  AND "INV_Motor_Speed" IS NOT NULL '
                f"LIMIT 1"
            ),
            mode="pandas",
        )
        return len(result) > 0
    except Exception:
        return False


@unittest.skipUnless(_influx_available(), "Live InfluxDB not available or database not found")
class TestExamples(unittest.TestCase):
    def test_end_to_end_example(self):
        """
        Runs examples/end_to_end.py to ensure the documentation example is valid.
        """
        example_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../examples/end_to_end.py'))
        
        # Set env var so the script doesn't block on plt.show()
        os.environ["TEST_MODE"] = "1"
        
        try:
            # Load the module dynamically
            spec = importlib.util.spec_from_file_location("end_to_end", example_path)
            end_to_end = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(end_to_end)
            
            # Run the main function
            print("\n[Test] Running Example Script: end_to_end.py")
            end_to_end.main()
            print("[Test] Example script finished successfully.")
            
        except Exception as e:
            self.fail(f"Example script failed with error: {e}")
        finally:
            # Cleanup
            if "TEST_MODE" in os.environ:
                del os.environ["TEST_MODE"]
            if os.path.exists("ci_plot_output.png"):
                os.remove("ci_plot_output.png")

if __name__ == '__main__':
    unittest.main()
