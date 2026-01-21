import pytest
from core.schema_ir import RoutineIR, RoutineCapability
from core.routine_translator import RoutineTranslator

class TestRoutineClassification:
    
    def setup_method(self):
        self.translator = RoutineTranslator(source_dialect="oracle", target_dialect="postgres")

    def test_analyze_unsafe_features(self):
        """Verify that unsafe features are detected"""
        ir = RoutineIR(
            name="unsafe_proc",
            arguments=[],
            body_source="BEGIN EXECUTE IMMEDIATE 'DROP TABLE x'; END;"
        )
        
        result = self.translator.process(ir, RoutineCapability.ANALYZE)
        
        assert result.outcome == "ANALYZED_ONLY"
        assert ir.risk_score >= 50
        assert any("Dynamic SQL" in issue for issue in ir.issues)
