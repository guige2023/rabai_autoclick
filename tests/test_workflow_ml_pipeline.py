"""
WorkflowMLPipeline 测试
ML Pipeline Orchestration 测试
"""
import unittest
import json
import hashlib
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from dataclasses import asdict

import sys
import os
sys.path.insert(0, '/Users/guige/my_project')

from src.workflow_ml_pipeline import (
    MLPipeline,
    PipelineStage,
    PipelineConfig,
    ModelVersion,
    ABTestResult,
    DataPreprocessor,
    FeatureEngineer,
    ModelTrainer,
    ModelEvaluator,
    HyperparameterTuner,
    ModelRegistry,
    PipelineVersioner,
    ABTester,
    PipelineScheduler,
    create_pipeline,
)


class TestPipelineStage(unittest.TestCase):
    """测试流水线阶段枚举"""
    
    def test_pipeline_stages(self):
        """测试所有流水线阶段"""
        self.assertEqual(PipelineStage.PREPROCESSING.value, "preprocessing")
        self.assertEqual(PipelineStage.FEATURE_ENGINEERING.value, "feature_engineering")
        self.assertEqual(PipelineStage.TRAINING.value, "training")
        self.assertEqual(PipelineStage.EVALUATION.value, "evaluation")
        self.assertEqual(PipelineStage.HYPERPARAMETER_TUNING.value, "hyperparameter_tuning")
        self.assertEqual(PipelineStage.REGISTERED.value, "registered")
        self.assertEqual(PipelineStage.A_B_TESTING.value, "a_b_testing")
        self.assertEqual(PipelineStage.SCHEDULED.value, "scheduled")


class TestPipelineConfig(unittest.TestCase):
    """测试流水线配置"""
    
    def test_create_config(self):
        """测试创建流水线配置"""
        config = PipelineConfig(
            stages=[PipelineStage.PREPROCESSING, PipelineStage.TRAINING],
            hyperparameters={"lr": 0.001},
            preprocessing_params={"normalize": True},
            feature_params={"features": ["f1", "f2"]},
            training_params={"epochs": 100},
            evaluation_params={"metric": "accuracy"}
        )
        
        self.assertEqual(len(config.stages), 2)
        self.assertEqual(config.hyperparameters, {"lr": 0.001})
    
    def test_config_defaults(self):
        """测试配置默认值"""
        config = PipelineConfig(stages=[PipelineStage.TRAINING])
        
        self.assertEqual(config.hyperparameters, {})
        self.assertEqual(config.preprocessing_params, {})
        self.assertEqual(config.feature_params, {})
        self.assertEqual(config.training_params, {})
        self.assertEqual(config.evaluation_params, {})
        self.assertIsNone(config.schedule)


class TestModelVersion(unittest.TestCase):
    """测试模型版本"""
    
    def test_create_model_version(self):
        """测试创建模型版本"""
        version = ModelVersion(
            version_id="v1",
            model_name="test_model",
            stage=PipelineStage.REGISTERED,
            metrics={"accuracy": 0.95},
            params={"lr": 0.001},
            created_at="2024-01-01T00:00:00",
            pipeline_config={},
            artifact_path="/path/to/model"
        )
        
        self.assertEqual(version.version_id, "v1")
        self.assertEqual(version.model_name, "test_model")
        self.assertEqual(version.stage, PipelineStage.REGISTERED)
        self.assertEqual(version.metrics["accuracy"], 0.95)


class TestABTestResult(unittest.TestCase):
    """测试A/B测试结果"""
    
    def test_create_ab_test_result(self):
        """测试创建A/B测试结果"""
        result = ABTestResult(
            test_id="ab_1",
            control_version_id="v1",
            treatment_version_id="v2",
            control_metrics={"accuracy": 0.8},
            treatment_metrics={"accuracy": 0.85},
            metric_deltas={"accuracy": 0.05},
            winner="treatment",
            confidence=0.95,
            started_at="2024-01-01T00:00:00"
        )
        
        self.assertEqual(result.test_id, "ab_1")
        self.assertEqual(result.winner, "treatment")
        self.assertEqual(result.confidence, 0.95)


class TestDataPreprocessor(unittest.TestCase):
    """测试数据预处理器"""
    
    def test_create_preprocessor(self):
        """测试创建预处理器"""
        prep = DataPreprocessor({"normalize": True})
        self.assertEqual(prep.params, {"normalize": True})
        self.assertFalse(prep._fitted)
    
    def test_fit(self):
        """测试拟合"""
        prep = DataPreprocessor()
        result = prep.fit({"data": "test"})
        self.assertEqual(result, prep)
        self.assertTrue(prep._fitted)
    
    def test_transform_without_fit(self):
        """测试未拟合时转换"""
        prep = DataPreprocessor()
        with self.assertRaises(RuntimeError):
            prep.transform({"data": "test"})
    
    def test_fit_transform(self):
        """测试拟合和转换"""
        prep = DataPreprocessor()
        result = prep.fit_transform({"data": "test"})
        self.assertTrue(prep._fitted)
        self.assertEqual(result, {"data": "test"})


class TestFeatureEngineer(unittest.TestCase):
    """测试特征工程"""
    
    def test_create_feature_engineer(self):
        """测试创建特征工程师"""
        fe = FeatureEngineer({"method": "standard"})
        self.assertEqual(fe.params, {"method": "standard"})
        self.assertEqual(fe._features_created, [])
    
    def test_create_features(self):
        """测试创建特征"""
        fe = FeatureEngineer()
        result = fe.create_features({"raw": "data"})
        self.assertEqual(fe._features_created, ["feature_1", "feature_2"])
    
    def test_get_feature_names(self):
        """测试获取特征名称"""
        fe = FeatureEngineer()
        fe.create_features({"data": "test"})
        names = fe.get_feature_names()
        self.assertEqual(names, ["feature_1", "feature_2"])


class TestModelTrainer(unittest.TestCase):
    """测试模型训练器"""
    
    def test_create_trainer(self):
        """测试创建训练器"""
        trainer = ModelTrainer({"epochs": 100})
        self.assertEqual(trainer.params, {"epochs": 100})
        self.assertFalse(trainer._trained)
        self.assertIsNone(trainer._model)
    
    def test_train(self):
        """测试训练"""
        trainer = ModelTrainer({"lr": 0.001})
        model = trainer.train({"X": "data"}, {"y": "label"})
        self.assertTrue(trainer._trained)
        self.assertEqual(model["trained"], True)
        self.assertEqual(model["params"], {"lr": 0.001})
    
    def test_predict_without_training(self):
        """测试未训练时预测"""
        trainer = ModelTrainer()
        with self.assertRaises(RuntimeError):
            trainer.predict({"X": "data"})
    
    def test_predict_after_training(self):
        """测试训练后预测"""
        trainer = ModelTrainer()
        trainer.train({"X": "data"}, {"y": "label"})
        result = trainer.predict({"X": "test"})
        self.assertEqual(result, {"X": "test"})


class TestModelEvaluator(unittest.TestCase):
    """测试模型评估器"""
    
    def test_create_evaluator(self):
        """测试创建评估器"""
        eval = ModelEvaluator({"metric": "f1"})
        self.assertEqual(eval.params, {"metric": "f1"})
    
    def test_evaluate(self):
        """测试评估"""
        eval = ModelEvaluator()
        metrics = eval.evaluate({"model": "test"}, {"X": "data"}, {"y": "labels"})
        
        self.assertIn("accuracy", metrics)
        self.assertIn("precision", metrics)
        self.assertIn("recall", metrics)
        self.assertIn("f1", metrics)
        self.assertIn("roc_auc", metrics)
    
    def test_cross_validate(self):
        """测试交叉验证"""
        eval = ModelEvaluator()
        result = eval.cross_validate({"model": "test"}, {"X": "data"}, {"y": "labels"}, cv=5)
        
        self.assertIn("cv_accuracy_mean", result)
        self.assertIn("cv_accuracy_std", result)


class TestHyperparameterTuner(unittest.TestCase):
    """测试超参数调优器"""
    
    def test_create_tuner(self):
        """测试创建调优器"""
        tuner = HyperparameterTuner({"method": "grid"})
        self.assertEqual(tuner.params, {"method": "grid"})
    
    def test_tune(self):
        """测试调优"""
        tuner = HyperparameterTuner()
        trainer = ModelTrainer()
        
        param_space = {
            "lr": [0.001, 0.01],
            "max_depth": [3, 5]
        }
        
        result = tuner.tune(trainer, {"X": "data"}, {"y": "labels"}, param_space)
        
        self.assertIn("best_params", result)
        self.assertIn("best_score", result)
        self.assertIn("search_history", result)


class TestModelRegistry(unittest.TestCase):
    """测试模型注册表"""
    
    def setUp(self):
        """设置"""
        self.registry = ModelRegistry()
    
    def test_register_model(self):
        """测试注册模型"""
        version = ModelVersion(
            version_id="v1",
            model_name="model_1",
            stage=PipelineStage.REGISTERED,
            metrics={"accuracy": 0.9},
            params={},
            created_at="2024-01-01T00:00:00",
            pipeline_config={}
        )
        
        vid = self.registry.register("model_1", version)
        self.assertEqual(vid, "v1")
        self.assertEqual(len(self.registry._models), 1)
    
    def test_get_model(self):
        """测试获取模型"""
        version = ModelVersion(
            version_id="v1",
            model_name="model_1",
            stage=PipelineStage.REGISTERED,
            metrics={},
            params={},
            created_at="2024-01-01T00:00:00",
            pipeline_config={}
        )
        self.registry.register("model_1", version)
        
        retrieved = self.registry.get("v1")
        self.assertEqual(retrieved.version_id, "v1")
    
    def test_get_latest(self):
        """测试获取最新版本"""
        v1 = ModelVersion(
            version_id="v1",
            model_name="model_1",
            stage=PipelineStage.REGISTERED,
            metrics={},
            params={},
            created_at="2024-01-01T00:00:00",
            pipeline_config={}
        )
        v2 = ModelVersion(
            version_id="v2",
            model_name="model_1",
            stage=PipelineStage.REGISTERED,
            metrics={},
            params={},
            created_at="2024-01-02T00:00:00",  # Later
            pipeline_config={}
        )
        
        self.registry.register("model_1", v1)
        self.registry.register("model_1", v2)
        
        latest = self.registry.get_latest("model_1")
        self.assertEqual(latest.version_id, "v2")
    
    def test_list_versions(self):
        """测试列出版本"""
        v1 = ModelVersion(version_id="v1", model_name="m1", stage=PipelineStage.REGISTERED,
                         metrics={}, params={}, created_at="2024-01-01", pipeline_config={})
        v2 = ModelVersion(version_id="v2", model_name="m1", stage=PipelineStage.REGISTERED,
                         metrics={}, params={}, created_at="2024-01-02", pipeline_config={})
        
        self.registry.register("m1", v1)
        self.registry.register("m1", v2)
        
        versions = self.registry.list_versions("m1")
        self.assertEqual(len(versions), 2)
        
        all_versions = self.registry.list_versions()
        self.assertEqual(len(all_versions), 2)
    
    def test_stage_model(self):
        """测试更新模型阶段"""
        version = ModelVersion(
            version_id="v1",
            model_name="model_1",
            stage=PipelineStage.REGISTERED,
            metrics={},
            params={},
            created_at="2024-01-01T00:00:00",
            pipeline_config={}
        )
        self.registry.register("model_1", version)
        
        self.registry.stage_model("v1", PipelineStage.PRODUCTION)
        self.assertEqual(self.registry.get("v1").stage, PipelineStage.PRODUCTION)
    
    def test_delete_model(self):
        """测试删除模型"""
        version = ModelVersion(
            version_id="v1",
            model_name="model_1",
            stage=PipelineStage.REGISTERED,
            metrics={},
            params={},
            created_at="2024-01-01T00:00:00",
            pipeline_config={}
        )
        self.registry.register("model_1", version)
        
        result = self.registry.delete("v1")
        self.assertTrue(result)
        self.assertIsNone(self.registry.get("v1"))
    
    def test_delete_nonexistent(self):
        """测试删除不存在的模型"""
        result = self.registry.delete("nonexistent")
        self.assertFalse(result)


class TestPipelineVersioner(unittest.TestCase):
    """测试流水线版本控制"""
    
    def setUp(self):
        """设置"""
        self.versioner = PipelineVersioner()
    
    def test_create_version(self):
        """测试创建版本"""
        config = PipelineConfig(stages=[PipelineStage.TRAINING])
        vid = self.versioner.create_version(config)
        
        self.assertIsNotNone(vid)
        self.assertTrue(vid.startswith("pipeline_"))
        self.assertEqual(len(self.versioner._versions), 1)
    
    def test_create_version_with_parent(self):
        """测试带父版本创建"""
        config1 = PipelineConfig(stages=[PipelineStage.TRAINING])
        vid1 = self.versioner.create_version(config1)
        
        config2 = PipelineConfig(stages=[PipelineStage.TRAINING, PipelineStage.EVALUATION])
        vid2 = self.versioner.create_version(config2, parent_id=vid1)
        
        self.assertEqual(len(self.versioner._versions), 2)
        
        v2_data = self.versioner.get_version(vid2)
        self.assertEqual(v2_data["parent_id"], vid1)
    
    def test_get_version(self):
        """测试获取版本"""
        config = PipelineConfig(stages=[PipelineStage.TRAINING])
        vid = self.versioner.create_version(config)
        
        version_data = self.versioner.get_version(vid)
        self.assertIsNotNone(version_data)
        self.assertIn("config", version_data)
    
    def test_list_versions(self):
        """测试列出版本"""
        config = PipelineConfig(stages=[PipelineStage.TRAINING])
        vid1 = self.versioner.create_version(config)
        vid2 = self.versioner.create_version(config)
        
        versions = self.versioner.list_versions()
        self.assertEqual(len(versions), 2)
    
    def test_compare_versions(self):
        """测试比较版本"""
        config1 = PipelineConfig(stages=[PipelineStage.TRAINING])
        config2 = PipelineConfig(stages=[PipelineStage.TRAINING, PipelineStage.EVALUATION])
        
        vid1 = self.versioner.create_version(config1)
        vid2 = self.versioner.create_version(config2)
        
        diff = self.versioner.compare_versions(vid1, vid2)
        
        self.assertIn("config_diff", diff)
        self.assertIn("stages", diff["config_diff"])


class TestABTester(unittest.TestCase):
    """测试A/B测试器"""
    
    def setUp(self):
        """设置"""
        self.tester = ABTester()
    
    def test_create_test(self):
        """测试创建测试"""
        test_id = self.tester.create_test(
            control_version_id="v1",
            treatment_version_id="v2",
            metrics=["accuracy", "precision"]
        )
        
        self.assertTrue(test_id.startswith("ab_test_"))
        self.assertEqual(len(self.tester._tests), 1)
    
    def test_record_metric(self):
        """测试记录指标"""
        test_id = self.tester.create_test(
            control_version_id="v1",
            treatment_version_id="v2",
            metrics=["accuracy"]
        )
        
        self.tester.record_metric(test_id, "control", "accuracy", 0.8)
        self.tester.record_metric(test_id, "treatment", "accuracy", 0.85)
        
        test = self.tester.get_test(test_id)
        self.assertEqual(test.control_metrics["accuracy"], 0.8)
        self.assertEqual(test.treatment_metrics["accuracy"], 0.85)
    
    def test_record_metric_invalid_test(self):
        """测试为无效测试记录指标"""
        with self.assertRaises(ValueError):
            self.tester.record_metric("nonexistent", "control", "accuracy", 0.8)
    
    def test_compute_results(self):
        """测试计算结果"""
        test_id = self.tester.create_test(
            control_version_id="v1",
            treatment_version_id="v2",
            metrics=["accuracy"]
        )
        
        self.tester.record_metric(test_id, "control", "accuracy", 0.8)
        self.tester.record_metric(test_id, "treatment", "accuracy", 0.85)
        
        result = self.tester.compute_results(test_id)
        
        self.assertEqual(result.winner, "treatment")
        self.assertEqual(result.metric_deltas["accuracy"], 0.05)
        self.assertIsNotNone(result.completed_at)
    
    def test_get_test(self):
        """测试获取测试"""
        test_id = self.tester.create_test("v1", "v2", ["accuracy"])
        test = self.tester.get_test(test_id)
        self.assertEqual(test.test_id, test_id)
    
    def test_list_tests(self):
        """测试列出测试"""
        self.tester.create_test("v1", "v2", ["accuracy"])
        self.tester.create_test("v3", "v4", ["precision"])
        
        tests = self.tester.list_tests()
        self.assertEqual(len(tests), 2)


class TestPipelineScheduler(unittest.TestCase):
    """测试流水线调度器"""
    
    def setUp(self):
        """设置"""
        self.scheduler = PipelineScheduler()
    
    def test_schedule(self):
        """测试调度"""
        schedule_id = self.scheduler.schedule(
            "pipeline_v1",
            {"cron": "0 9 * * *", "enabled": True}
        )
        
        self.assertTrue(schedule_id.startswith("schedule_"))
        self.assertEqual(len(self.scheduler._schedules), 1)
    
    def test_unschedule(self):
        """测试取消调度"""
        schedule_id = self.scheduler.schedule("pipeline_v1", {})
        result = self.scheduler.unschedule(schedule_id)
        
        self.assertTrue(result)
        self.assertEqual(len(self.scheduler._schedules), 0)
    
    def test_unschedule_nonexistent(self):
        """测试取消不存在的调度"""
        result = self.scheduler.unschedule("nonexistent")
        self.assertFalse(result)
    
    def test_trigger_run(self):
        """测试触发运行"""
        schedule_id = self.scheduler.schedule("pipeline_v1", {})
        run_id = self.scheduler.trigger_run(schedule_id)
        
        self.assertTrue(run_id.startswith("run_"))
        self.assertEqual(len(self.scheduler._execution_history), 1)
    
    def test_trigger_run_invalid_schedule(self):
        """测试为无效调度触发运行"""
        with self.assertRaises(ValueError):
            self.scheduler.trigger_run("nonexistent")
    
    def test_complete_run(self):
        """测试完成运行"""
        schedule_id = self.scheduler.schedule("pipeline_v1", {})
        run_id = self.scheduler.trigger_run(schedule_id)
        
        self.scheduler.complete_run(run_id, "success")
        
        runs = self.scheduler.list_runs()
        self.assertEqual(runs[0]["status"], "success")
        self.assertIsNotNone(runs[0]["completed_at"])
    
    def test_get_schedule(self):
        """测试获取调度"""
        schedule_id = self.scheduler.schedule("pipeline_v1", {"cron": "0 9 * * *"})
        schedule = self.scheduler.get_schedule(schedule_id)
        
        self.assertEqual(schedule["pipeline_version_id"], "pipeline_v1")
        self.assertEqual(schedule["cron"], "0 9 * * *")
    
    def test_list_schedules(self):
        """测试列出调度"""
        self.scheduler.schedule("pipeline_v1", {})
        self.scheduler.schedule("pipeline_v2", {})
        
        schedules = self.scheduler.list_schedules()
        self.assertEqual(len(schedules), 2)
    
    def test_list_runs(self):
        """测试列出运行"""
        schedule_id = self.scheduler.schedule("pipeline_v1", {})
        self.scheduler.trigger_run(schedule_id)
        self.scheduler.trigger_run(schedule_id)
        
        runs = self.scheduler.list_runs(schedule_id)
        self.assertEqual(len(runs), 2)
        
        all_runs = self.scheduler.list_runs()
        self.assertEqual(len(all_runs), 2)


class TestMLPipeline(unittest.TestCase):
    """测试MLPipeline主类"""
    
    def setUp(self):
        """设置"""
        self.pipeline = MLPipeline(name="test_pipeline")
    
    def test_initialization(self):
        """测试初始化"""
        self.assertEqual(self.pipeline.name, "test_pipeline")
        self.assertIsNone(self.pipeline.current_config)
        self.assertIsNone(self.pipeline.current_version_id)
        self.assertIsNone(self.pipeline._trained_model)
        self.assertEqual(self.pipeline._latest_metrics, {})
    
    def test_initialization_with_default_components(self):
        """测试带默认组件初始化"""
        self.assertIsInstance(self.pipeline.preprocessor, DataPreprocessor)
        self.assertIsInstance(self.pipeline.feature_engineer, FeatureEngineer)
        self.assertIsInstance(self.pipeline.trainer, ModelTrainer)
        self.assertIsInstance(self.pipeline.evaluator, ModelEvaluator)
        self.assertIsInstance(self.pipeline.tuner, HyperparameterTuner)
        self.assertIsInstance(self.pipeline.registry, ModelRegistry)
        self.assertIsInstance(self.pipeline.versioner, PipelineVersioner)
        self.assertIsInstance(self.pipeline.ab_tester, ABTester)
        self.assertIsInstance(self.pipeline.scheduler, PipelineScheduler)
    
    def test_define_stages(self):
        """测试定义阶段"""
        stages = [PipelineStage.PREPROCESSING, PipelineStage.TRAINING]
        result = self.pipeline.define_stages(stages)
        
        self.assertEqual(result, self.pipeline)  # Should return self for chaining
        self.assertEqual(self.pipeline.current_config.stages, stages)
    
    def test_add_stage(self):
        """测试添加阶段"""
        self.pipeline.define_stages([PipelineStage.PREPROCESSING])
        self.pipeline.add_stage(PipelineStage.TRAINING)
        
        self.assertEqual(len(self.pipeline.current_config.stages), 2)
    
    def test_add_stage_without_existing_config(self):
        """测试在无配置时添加阶段"""
        result = self.pipeline.add_stage(PipelineStage.TRAINING)
        
        self.assertIsNotNone(self.pipeline.current_config)
        self.assertEqual(len(self.pipeline.current_config.stages), 1)
    
    def test_preprocess_data(self):
        """测试数据预处理"""
        data = {"raw": "test_data"}
        result = self.pipeline.preprocess_data(data)
        
        self.assertEqual(result, data)
    
    def test_engineer_features(self):
        """测试特征工程"""
        data = {"raw": "test_data"}
        result = self.pipeline.engineer_features(data)
        
        self.assertEqual(result, data)
    
    def test_train_model(self):
        """测试训练模型"""
        X = {"features": [1, 2, 3]}
        y = {"labels": [0, 1, 0]}
        
        result = self.pipeline.train_model(X, y)
        
        self.assertIsNotNone(self.pipeline._trained_model)
        self.assertTrue(self.pipeline._trained_model["trained"])
    
    def test_evaluate_model(self):
        """测试评估模型"""
        X = {"features": [1, 2, 3]}
        y = {"labels": [0, 1, 0]}
        
        self.pipeline.train_model(X, y)
        metrics = self.pipeline.evaluate_model(X, y)
        
        self.assertIn("accuracy", metrics)
        self.assertEqual(self.pipeline._latest_metrics, metrics)
    
    def test_tune_hyperparameters(self):
        """测试超参数调优"""
        X = {"features": [1, 2, 3]}
        y = {"labels": [0, 1, 0]}
        
        self.pipeline.train_model(X, y)
        result = self.pipeline.tune_hyperparameters(X, y)
        
        self.assertIn("best_params", result)
        self.assertIn("best_score", result)
    
    def test_register_model_without_training(self):
        """测试未训练时注册模型"""
        with self.assertRaises(RuntimeError):
            self.pipeline.register_model("test_model")
    
    def test_register_model(self):
        """测试注册模型"""
        X = {"features": [1, 2, 3]}
        y = {"labels": [0, 1, 0]}
        
        self.pipeline.train_model(X, y)
        self.pipeline.evaluate_model(X, y)
        
        vid = self.pipeline.register_model("test_model")
        
        self.assertIsNotNone(vid)
        self.assertEqual(len(self.pipeline.registry._models), 1)
    
    def test_get_registered_model(self):
        """测试获取注册模型"""
        X = {"features": [1, 2, 3]}
        y = {"labels": [0, 1, 0]}
        
        self.pipeline.train_model(X, y)
        self.pipeline.evaluate_model(X, y)
        
        vid = self.pipeline.register_model("test_model")
        model = self.pipeline.get_registered_model(vid)
        
        self.assertEqual(model.version_id, vid)
        self.assertEqual(model.model_name, "test_model")
    
    def test_list_registered_models(self):
        """测试列出注册模型"""
        X = {"features": [1, 2, 3]}
        y = {"labels": [0, 1, 0]}
        
        self.pipeline.train_model(X, y)
        self.pipeline.evaluate_model(X, y)
        
        self.pipeline.register_model("model_1")
        models = self.pipeline.list_registered_models()
        
        self.assertEqual(len(models), 1)
    
    def test_stage_model(self):
        """测试更新模型阶段"""
        X = {"features": [1, 2, 3]}
        y = {"labels": [0, 1, 0]}
        
        self.pipeline.train_model(X, y)
        self.pipeline.evaluate_model(X, y)
        
        vid = self.pipeline.register_model("test_model")
        self.pipeline.stage_model(vid, PipelineStage.PRODUCTION)
        
        model = self.pipeline.get_registered_model(vid)
        self.assertEqual(model.stage, PipelineStage.PRODUCTION)
    
    def test_create_pipeline_version(self):
        """测试创建流水线版本"""
        self.pipeline.define_stages([PipelineStage.TRAINING])
        vid = self.pipeline.create_pipeline_version()
        
        self.assertIsNotNone(vid)
        self.assertEqual(self.pipeline.current_version_id, vid)
    
    def test_get_pipeline_version(self):
        """测试获取流水线版本"""
        self.pipeline.define_stages([PipelineStage.TRAINING])
        vid = self.pipeline.create_pipeline_version()
        
        version_data = self.pipeline.get_pipeline_version()
        self.assertIsNotNone(version_data)
    
    def test_compare_pipeline_versions(self):
        """测试比较流水线版本"""
        self.pipeline.define_stages([PipelineStage.TRAINING])
        vid1 = self.pipeline.create_pipeline_version()
        
        self.pipeline.add_stage(PipelineStage.EVALUATION)
        vid2 = self.pipeline.create_pipeline_version()
        
        diff = self.pipeline.compare_pipeline_versions(vid1, vid2)
        self.assertIn("config_diff", diff)
    
    def test_create_ab_test(self):
        """测试创建A/B测试"""
        self.pipeline.define_stages([PipelineStage.TRAINING])
        vid1 = self.pipeline.create_pipeline_version()
        vid2 = self.pipeline.create_pipeline_version()
        
        test_id = self.pipeline.create_ab_test(vid1, vid2, ["accuracy"])
        
        self.assertTrue(test_id.startswith("ab_test_"))
    
    def test_record_ab_metric(self):
        """测试记录A/B测试指标"""
        self.pipeline.define_stages([PipelineStage.TRAINING])
        vid1 = self.pipeline.create_pipeline_version()
        vid2 = self.pipeline.create_pipeline_version()
        
        test_id = self.pipeline.create_ab_test(vid1, vid2)
        self.pipeline.record_ab_metric(test_id, "control", "accuracy", 0.8)
        self.pipeline.record_ab_metric(test_id, "treatment", "accuracy", 0.85)
        
        test = self.pipeline.get_ab_test(test_id)
        self.assertEqual(test.control_metrics["accuracy"], 0.8)
    
    def test_compute_ab_test_results(self):
        """测试计算A/B测试结果"""
        self.pipeline.define_stages([PipelineStage.TRAINING])
        vid1 = self.pipeline.create_pipeline_version()
        vid2 = self.pipeline.create_pipeline_version()
        
        test_id = self.pipeline.create_ab_test(vid1, vid2)
        self.pipeline.record_ab_metric(test_id, "control", "accuracy", 0.8)
        self.pipeline.record_ab_metric(test_id, "treatment", "accuracy", 0.85)
        
        result = self.pipeline.compute_ab_test_results(test_id)
        
        self.assertEqual(result.winner, "treatment")
    
    def test_list_ab_tests(self):
        """测试列出A/B测试"""
        self.pipeline.define_stages([PipelineStage.TRAINING])
        vid1 = self.pipeline.create_pipeline_version()
        vid2 = self.pipeline.create_pipeline_version()
        
        self.pipeline.create_ab_test(vid1, vid2)
        self.pipeline.create_ab_test(vid2, vid1)
        
        tests = self.pipeline.list_ab_tests()
        self.assertEqual(len(tests), 2)
    
    def test_schedule_pipeline(self):
        """测试调度流水线"""
        schedule_id = self.pipeline.schedule_pipeline({"cron": "0 9 * * *"})
        
        self.assertTrue(schedule_id.startswith("schedule_"))
    
    def test_trigger_scheduled_run(self):
        """测试触发调度运行"""
        schedule_id = self.pipeline.schedule_pipeline({})
        run_id = self.pipeline.trigger_scheduled_run(schedule_id)
        
        self.assertTrue(run_id.startswith("run_"))
    
    def test_complete_pipeline_run(self):
        """测试完成流水线运行"""
        schedule_id = self.pipeline.schedule_pipeline({})
        run_id = self.pipeline.trigger_scheduled_run(schedule_id)
        
        self.pipeline.complete_pipeline_run(run_id, "success")
        
        schedule = self.pipeline.get_schedule(schedule_id)
        self.assertIsNotNone(schedule["last_run"])
    
    def test_get_schedule(self):
        """测试获取调度"""
        schedule_id = self.pipeline.schedule_pipeline({"cron": "0 9 * * *"})
        
        schedule = self.pipeline.get_schedule(schedule_id)
        self.assertIsNotNone(schedule)
    
    def test_list_schedules(self):
        """测试列出调度"""
        self.pipeline.schedule_pipeline({"cron": "0 9 * * *"})
        self.pipeline.schedule_pipeline({"cron": "0 10 * * *"})
        
        schedules = self.pipeline.list_schedules()
        self.assertEqual(len(schedules), 2)
    
    def test_run_full_pipeline(self):
        """测试完整流水线运行"""
        X = {"features": [1, 2, 3]}
        y = {"labels": [0, 1, 0]}
        
        self.pipeline.define_stages([
            PipelineStage.PREPROCESSING,
            PipelineStage.FEATURE_ENGINEERING,
            PipelineStage.TRAINING,
            PipelineStage.EVALUATION
        ])
        
        results = self.pipeline.run_full_pipeline(X, y, "full_test_model")
        
        self.assertIn("stages_completed", results)
        self.assertIn("metrics", results)
        self.assertIn("version_id", results)
        self.assertIn("pipeline_version_id", results)
        self.assertEqual(len(results["stages_completed"]), 4)
    
    def test_run_full_pipeline_default_stages(self):
        """测试使用默认阶段的完整流水线"""
        X = {"features": [1, 2, 3]}
        y = {"labels": [0, 1, 0]}
        
        results = self.pipeline.run_full_pipeline(X, y)
        
        self.assertIn("stages_completed", results)
    
    def test_get_pipeline_info(self):
        """测试获取流水线信息"""
        info = self.pipeline.get_pipeline_info()
        
        self.assertEqual(info["name"], "test_pipeline")
        self.assertFalse(info["has_trained_model"])
        self.assertEqual(info["registered_model_count"], 0)
        self.assertEqual(info["pipeline_version_count"], 0)
    
    def test_pipeline_info_after_training(self):
        """测试训练后获取流水线信息"""
        X = {"features": [1, 2, 3]}
        y = {"labels": [0, 1, 0]}
        
        self.pipeline.train_model(X, y)
        self.pipeline.evaluate_model(X, y)
        self.pipeline.register_model("model_1")
        
        info = self.pipeline.get_pipeline_info()
        
        self.assertTrue(info["has_trained_model"])
        self.assertEqual(info["registered_model_count"], 1)


class TestCreatePipeline(unittest.TestCase):
    """测试创建流水线工厂函数"""
    
    def test_create_pipeline_default(self):
        """测试创建默认配置流水线"""
        pipeline = create_pipeline("my_pipeline")
        
        self.assertEqual(pipeline.name, "my_pipeline")
        self.assertIsNotNone(pipeline.current_config)
    
    def test_create_pipeline_with_stages(self):
        """测试创建指定阶段的流水线"""
        stages = [PipelineStage.PREPROCESSING, PipelineStage.TRAINING]
        pipeline = create_pipeline("custom_pipeline", stages=stages)
        
        self.assertEqual(len(pipeline.current_config.stages), len(stages))
    
    def test_create_pipeline_with_default_stages(self):
        """测试创建带默认阶段的流水线"""
        pipeline = create_pipeline("default_pipeline")
        
        expected_stages = [
            PipelineStage.PREPROCESSING,
            PipelineStage.FEATURE_ENGINEERING,
            PipelineStage.TRAINING,
            PipelineStage.EVALUATION,
            PipelineStage.HYPERPARAMETER_TUNING
        ]
        
        self.assertEqual(pipeline.current_config.stages, expected_stages)


class TestPipelineChaining(unittest.TestCase):
    """测试流水线方法链式调用"""
    
    def test_method_chaining(self):
        """测试方法链式调用"""
        pipeline = MLPipeline("chained_pipeline")
        
        result = (pipeline
            .define_stages([PipelineStage.PREPROCESSING, PipelineStage.TRAINING])
            .add_stage(PipelineStage.EVALUATION))
        
        self.assertEqual(result, pipeline)
        self.assertEqual(len(pipeline.current_config.stages), 3)


if __name__ == "__main__":
    unittest.main()
