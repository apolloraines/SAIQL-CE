import pytest
from core.schema_ir import RoutineIR, RoutineCapability, RoutineArgumentIR
from core.routine_translator import RoutineTranslator

class TestRoutineStubOutput:
    
    def setup_method(self):
        self.translator = RoutineTranslator(source_dialect="oracle", target_dialect="postgres")

    def test_stub_generation(self):
        """Verify stub generation works"""
        ir = RoutineIR(
            name="complex_proc",
            arguments=[RoutineArgumentIR(name="id", data_type="NUMBER", mode="IN")],
            return_type="NUMBER",
            body_source="BEGIN ... complex stuff ... END;"
        )
        
        # Force Stub
        result = self.translator.process(ir, RoutineCapability.STUB)
        
        assert result.outcome == "STUBBED"
        print(result.generated_code)
        assert "CREATE OR REPLACE FUNCTION complex_proc" in result.generated_code
        assert "RAISE EXCEPTION" in result.generated_code
        assert "id NUMERIC" in result.generated_code # Check type mapping NUMBER -> NUMERIC
