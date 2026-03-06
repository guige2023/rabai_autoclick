#!/usr/bin/env python3
"""
RabAI AutoClick v22 CLI
命令行接口 - 包含 v22 新增功能
- 预测性自动化引擎
- 故障自愈系统
- 场景化工作流包
- 增强版智能诊断室
- 无代码工作流分享
- CLI 管道集成
- 屏幕录制转工作流
"""
import sys
import os
import json
import click
from pathlib import Path

# 添加 src 目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.predictive_engine import create_predictive_engine
from src.self_healing_system import create_self_healing_system
from src.workflow_package import create_scene_manager
from src.workflow_diagnostics import create_diagnostics
from src.workflow_share import create_share_system, ShareType
from src.pipeline_mode import create_pipeline_runner, PipeCLI, PipeMode
from src.screen_recorder import create_screen_recorder


DATA_DIR = Path(__file__).parent.parent / "data"


@click.group()
@click.version_option(version="22.0.0")
def cli():
    """RabAI AutoClick v22 - 智能自动化工具
    
    差异化功能:
    - 预测性自动化引擎
    - 故障自愈系统
    - 场景化工作流包
    - 增强版智能诊断室
    - 无代码工作流分享 (v22新增)
    - CLI 管道集成模式 (v22新增)
    - 屏幕录制转自动化流程 (v22新增)
    """
    DATA_DIR.mkdir(exist_ok=True)


# ========== 预测性自动化引擎 ==========
@cli.group()
def predict():
    """预测性自动化引擎"""
    pass


@predict.command("record")
@click.argument("action_type")
@click.argument("target")
@click.option("--context", "-c", default="{}", help="上下文JSON")
@click.option("--result", default="success", help="执行结果")
@click.option("--duration", default=0.0, type=float, help="耗时(秒)")
def predict_record(action_type, target, context, result, duration):
    """记录用户动作"""
    engine = create_predictive_engine(str(DATA_DIR))
    ctx = json.loads(context) if context != "{}" else {}
    engine.record_action(action_type, target, ctx, result, duration)
    click.echo(f"✓ 已记录动作: {action_type} -> {target}")


@predict.command("next")
@click.option("--app", help="当前活动应用")
def predict_next(app):
    """预测下一个动作"""
    engine = create_predictive_engine(str(DATA_DIR))
    ctx = {"active_app": app} if app else {}
    prediction = engine.predict_next_action(ctx)
    
    if prediction:
        click.echo(f"\n🔮 预测动作: {prediction.predicted_action}")
        click.echo(f"   置信度: {prediction.confidence * 100:.1f}%")
        click.echo(f"   推理: {prediction.reasoning}")
        if prediction.alternatives:
            click.echo(f"   备选: {', '.join(prediction.alternatives)}")
    else:
        click.echo("暂无预测数据，请先记录更多动作")


@predict.command("suggest")
def predict_suggest():
    """获取创建工作流建议"""
    engine = create_predictive_engine(str(DATA_DIR))
    suggestion = engine.suggest_workflow_creation()
    if suggestion:
        click.echo(f"💡 {suggestion}")
    else:
        click.echo("暂无建议")


@predict.command("analyze")
def predict_analyze():
    """分析用户行为"""
    engine = create_predictive_engine(str(DATA_DIR))
    analysis = engine.analyze_user_behavior()
    
    click.echo("\n📊 用户行为分析")
    click.echo(f"  总动作数: {analysis.get('total_actions', 0)}")
    click.echo(f"  成功率: {analysis.get('success_rate', 0)*100:.1f}%")
    
    if "top_targets" in analysis:
        click.echo("\n  常用操作 TOP5:")
        for i, (target, count) in enumerate(analysis["top_targets"].items()[:5], 1):
            click.echo(f"    {i}. {target}: {count}次")


# ========== 故障自愈系统 ==========
@cli.group()
def heal():
    """故障自愈系统"""
    pass


@heal.command("fix")
@click.argument("workflow_name")
@click.argument("step_name")
@click.option("--error", "-e", required=True, help="错误信息")
@click.option("--step-index", "-i", default=0, type=int, help="步骤索引")
def heal_fix(workflow_name, step_name, error, step_index):
    """分析错误并获取修复建议"""
    system = create_self_healing_system(str(DATA_DIR))
    
    class MockError(Exception):
        pass
    
    err = MockError(error)
    record = system.analyze_error(err, workflow_name, step_name, step_index, {})
    suggestions = system.get_fix_suggestions(record)
    
    click.echo(f"\n🔧 错误分析: {error}")
    click.echo(f"   类型: {record.error_type.value}")
    
    if suggestions:
        click.echo(f"\n💡 修复建议 ({len(suggestions)}条):")
        for i, s in enumerate(suggestions, 1):
            click.echo(f"  {i}. [{s.strategy.value}] {s.description}")
            click.echo(f"     实现: {s.implementation}")
    else:
        click.echo("暂无建议")


@heal.command("stats")
def heal_stats():
    """获取错误统计"""
    system = create_self_healing_system(str(DATA_DIR))
    stats = system.get_error_statistics()
    
    click.echo("\n📊 错误统计")
    click.echo(f"  总错误数: {stats.get('total_errors', 0)}")
    click.echo(f"  恢复成功率: {stats.get('recovery_rate', 0)*100:.1f}%")
    
    if "error_type_distribution" in stats:
        click.echo("\n  错误类型分布:")
        for etype, count in stats["error_type_distribution"].items():
            click.echo(f"    - {etype}: {count}")


# ========== 场景化工作流包 ==========
@cli.group()
def scene():
    """场景化工作流包"""
    pass


@scene.command("list")
@click.option("--tag", help="按标签筛选")
def scene_list(tag):
    """列出所有场景"""
    manager = create_scene_manager(str(DATA_DIR))
    tags = [tag] if tag else None
    
    scenes = manager.list_scenes(tags=tags)
    
    if not scenes:
        click.echo("暂无场景")
        return
    
    click.echo(f"\n📦 场景列表 ({len(scenes)}个)")
    for s in scenes:
        status_icon = "✅" if s.status.value == "active" else "⏸️"
        click.echo(f"\n{status_icon} {s.icon} {s.name}")
        click.echo(f"   {s.description}")
        click.echo(f"   工作流: {len(s.workflows)}个, 使用: {s.usage_count}次")


@scene.command("activate")
@click.argument("scene_id")
def scene_activate(scene_id):
    """激活场景"""
    manager = create_scene_manager(str(DATA_DIR))
    
    if manager.activate_scene(scene_id):
        scene = manager.get_scene(scene_id)
        click.echo(f"✅ 已激活场景: {scene.name}")
    else:
        click.echo(f"❌ 场景不存在: {scene_id}")


@scene.command("create")
@click.argument("name")
@click.option("--description", "-d", default="", help="场景描述")
@click.option("--icon", "-i", default="📦", help="图标")
def scene_create(name, description, icon):
    """创建新场景"""
    manager = create_scene_manager(str(DATA_DIR))
    scene = manager.create_scene(name, description, icon)
    click.echo(f"✅ 已创建场景: {scene.name} (ID: {scene.scene_id})")


@scene.command("stats")
def scene_stats():
    """场景统计"""
    manager = create_scene_manager(str(DATA_DIR))
    stats = manager.get_scene_statistics()
    
    click.echo("\n📊 场景统计")
    click.echo(f"  总场景数: {stats['total_scenes']}")
    click.echo(f"  激活中: {stats['active_scenes']}")


# ========== 增强版智能诊断室 ==========
@cli.group()
def diag():
    """智能诊断室 v22 (增强版)"""
    pass


@diag.command("run")
@click.argument("workflow_id", required=False)
def diag_run(workflow_id):
    """运行增强诊断"""
    diag = create_diagnostics(str(DATA_DIR))
    
    if workflow_id:
        report = diag.diagnose(workflow_id)
        click.echo(diag.generate_report_text(report))
    else:
        reports = diag.get_all_workflows_health()
        click.echo(f"\n📊 工作流健康概览 ({len(reports)}个工作流)")
        
        for r in reports[:10]:
            emoji = "🟢" if r.health_score >= 75 else "🟡" if r.health_score >= 50 else "🔴"
            click.echo(f"  {emoji} {r.workflow_name}: {r.health_score:.1f}分 ({r.success_rate*100:.0f}%成功率)")


@diag.command("summary")
def diag_summary():
    """健康概览"""
    diag = create_diagnostics(str(DATA_DIR))
    summary = diag.get_health_summary()
    
    click.echo("\n📊 总体健康状态")
    click.echo(f"  工作流总数: {summary.get('total_workflows', 0)}")
    if 'avg_health_score' in summary:
        click.echo(f"  平均健康分: {summary['avg_health_score']:.1f}")
    if 'avg_success_rate' in summary:
        click.echo(f"  平均成功率: {summary['avg_success_rate']*100:.1f}%")
    
    if "health_distribution" in summary:
        click.echo("\n  健康分布:")
        for level, count in summary["health_distribution"].items():
            click.echo(f"    - {level}: {count}")
    
    if summary.get("needs_attention"):
        click.echo("\n  ⚠️ 需要关注:")
        for name in summary["needs_attention"]:
            click.echo(f"    - {name}")


@diag.command("report")
@click.argument("workflow_id")
def diag_report(workflow_id):
    """生成详细报告"""
    diag = create_diagnostics(str(DATA_DIR))
    report = diag.diagnose(workflow_id)
    
    report_file = DATA_DIR / f"report_{workflow_id}_{int(time.time())}.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump({
            "workflow_id": report.workflow_id,
            "workflow_name": report.workflow_name,
            "overall_health": report.overall_health.value,
            "health_score": report.health_score,
            "execution_count": report.execution_count,
            "success_rate": report.success_rate,
            "avg_duration": report.avg_duration,
            "trends": [
                {
                    "period": t.period,
                    "success_rate_change": t.success_rate_change,
                    "trend_direction": t.trend_direction
                }
                for t in report.trends
            ],
            "issues": [
                {
                    "type": i.issue_type,
                    "severity": i.severity.value,
                    "title": i.title,
                    "suggestion": i.suggestion,
                    "auto_fixable": i.auto_fixable
                }
                for i in report.issues
            ],
            "root_causes": report.root_causes,
            "recommendations": report.recommendations
        }, f, ensure_ascii=False, indent=2)
    
    click.echo(f"✅ 报告已保存: {report_file}")
    click.echo(diag.generate_report_text(report))


# ========== 无代码工作流分享系统 (v22新增) ==========
@cli.group()
def share():
    """无代码工作流分享系统 (v22新增)"""
    pass


@share.command("register")
@click.argument("workflow_file")
def share_register(workflow_file):
    """注册工作流"""
    share_sys = create_share_system(str(DATA_DIR))
    
    try:
        with open(workflow_file, "r", encoding="utf-8") as f:
            workflow_data = json.load(f)
        
        wf_id = share_sys.register_workflow(workflow_data)
        click.echo(f"✅ 已注册工作流: {wf_id}")
    except Exception as e:
        click.echo(f"❌ 注册失败: {e}")


@share.command("create-link")
@click.argument("workflow_id")
@click.option("--type", "-t", default="public", help="分享类型: public, private, team")
@click.option("--expires", "-e", type=int, help="过期天数")
def share_create_link(workflow_id, type, expires):
    """创建分享链接"""
    share_sys = create_share_system(str(DATA_DIR))
    
    share_type = ShareType.PUBLIC if type == "public" else ShareType.PRIVATE if type == "private" else ShareType.TEAM
    link = share_sys.create_share_link(workflow_id, share_type, expires)
    
    if link:
        url = share_sys.generate_share_url(link.link_id)
        click.echo(f"✅ 分享链接已创建:")
        click.echo(f"   {url}")
        click.echo(f"   链接ID: {link.link_id}")
        if link.expires_at:
            import datetime
            exp_date = datetime.datetime.fromtimestamp(link.expires_at)
            click.echo(f"   过期时间: {exp_date.strftime('%Y-%m-%d %H:%M')}")
    else:
        click.echo(f"❌ 工作流不存在: {workflow_id}")


@share.command("import")
@click.argument("source")
@click.option("--format", "-f", default="json", help="格式: json, base64, url")
def share_import(source, format):
    """导入工作流"""
    share_sys = create_share_system(str(DATA_DIR))
    
    if format == "url":
        report = share_sys.import_from_url(source)
    else:
        report = share_sys.import_workflow(source, format)
    
    click.echo(f"\n导入结果: {report.result.value}")
    click.echo(f"工作流: {report.workflow_name}")
    click.echo(f"消息: {report.message}")
    
    if report.warnings:
        click.echo("\n警告:")
        for w in report.warnings:
            click.echo(f"  - {w}")


@share.command("export")
@click.argument("workflow_id")
@click.option("--format", "-f", default="json", help="格式: json, base64")
def share_export(workflow_id, format):
    """导出工作流"""
    share_sys = create_share_system(str(DATA_DIR))
    
    if format == "base64":
        output = share_sys.export_to_base64(workflow_id)
    else:
        output = share_sys.export_to_json(workflow_id)
    
    if output:
        click.echo(f"✅ 工作流已导出:")
        if format == "base64":
            click.echo(output)
        else:
            click.echo(output[:500] + "..." if len(output) > 500 else output)
    else:
        click.echo(f"❌ 工作流不存在: {workflow_id}")


@share.command("list")
def share_list():
    """列出分享的工作流"""
    share_sys = create_share_system(str(DATA_DIR))
    links = share_sys.list_shared_workflows()
    
    if not links:
        click.echo("暂无分享链接")
        return
    
    click.echo(f"\n📤 分享列表 ({len(links)}个):")
    for link in links:
        click.echo(f"\n  {link.link_id}: {link.workflow_name}")
        click.echo(f"    类型: {link.share_type.value}, 查看: {link.view_count}, 导入: {link.import_count}")


@share.command("stats")
def share_stats():
    """分享统计"""
    share_sys = create_share_system(str(DATA_DIR))
    stats = share_sys.get_share_stats()
    
    click.echo("\n📊 分享统计")
    click.echo(f"  总链接数: {stats['total_links']}")
    click.echo(f"  总查看: {stats['total_views']}")
    click.echo(f"  总导入: {stats['total_imports']}")
    click.echo(f"  有效链接: {stats['active_links']}")


# ========== CLI 管道集成模式 (v22新增) ==========
@cli.group()
def pipe():
    """CLI 管道集成模式 (v22新增)"""
    pass


@pipe.command("list")
def pipe_list():
    """列出管道链"""
    runner = create_pipeline_runner(str(DATA_DIR))
    chains = runner.list_chains()
    
    if not chains:
        click.echo("暂无管道链")
        return
    
    click.echo(f"\n🔗 管道链 ({len(chains)}个):\n")
    for chain in chains:
        status = "active" if any(s.enabled for s in chain.steps) else "disabled"
        click.echo(f"  {chain.chain_id}: {chain.name} [{status}]")
        click.echo(f"    模式: {chain.mode.value}, 步骤: {len(chain.steps)}")


@pipe.command("create")
@click.argument("name")
@click.option("--mode", "-m", default="linear", help="模式: linear, branch, parallel")
def pipe_create(name, mode):
    """创建管道链"""
    runner = create_pipeline_runner(str(DATA_DIR))
    
    try:
        pipe_mode = PipeMode(mode)
    except ValueError:
        click.echo(f"❌ 无效模式: {mode}")
        return
    
    chain = runner.create_chain(name, pipe_mode)
    click.echo(f"✅ 已创建管道链: {chain.chain_id}")


@pipe.command("add")
@click.argument("chain_id")
@click.argument("name")
@click.argument("command")
def pipe_add(chain_id, name, command):
    """添加步骤"""
    runner = create_pipeline_runner(str(DATA_DIR))
    
    step = runner.add_step(chain_id, name, command)
    if step:
        click.echo(f"✅ 已添加步骤: {step.step_id}")
    else:
        click.echo(f"❌ 管道链不存在: {chain_id}")


@pipe.command("run")
@click.argument("chain_id")
@click.option("--input", "-i", help="输入JSON数据")
def pipe_run(chain_id, input):
    """运行管道链"""
    runner = create_pipeline_runner(str(DATA_DIR))
    
    input_data = None
    if input:
        try:
            input_data = json.loads(input)
        except json.JSONDecodeError:
            click.echo("❌ 无效的JSON输入")
            return
    
    result = runner.execute_chain(chain_id, input_data)
    
    click.echo(f"\n管道执行结果:")
    click.echo(f"  成功: {'✅' if result.success else '❌'}")
    click.echo(f"  耗时: {result.total_duration:.2f}秒")
    
    if result.final_output:
        click.echo(f"  输出:")
        click.echo(f"  {json.dumps(result.final_output, ensure_ascii=False, indent=4)}")
    
    if result.errors:
        click.echo(f"\n错误:")
        for err in result.errors:
            click.echo(f"  - {err}")


# ========== 屏幕录制转自动化流程 (v22新增) ==========
@cli.group()
def rec():
    """屏幕录制转自动化流程 (v22新增)"""
    pass


@rec.command("start")
@click.argument("name")
@click.option("--description", "-d", default="", help="描述")
def rec_start(name, description):
    """开始录制"""
    converter = create_screen_recorder(str(DATA_DIR))
    rec = converter.start_recording(name, description)
    click.echo(f"✅ 开始录制: {rec.recording_id}")
    click.echo(f"   名称: {rec.name}")


@rec.command("stop")
@click.argument("recording_id")
def rec_stop(recording_id):
    """停止录制"""
    converter = create_screen_recorder(str(DATA_DIR))
    rec = converter.stop_recording(recording_id)
    
    if rec:
        click.echo(f"✅ 录制停止: {rec.recording_id}")
        click.echo(f"   动作数: {len(rec.actions)}")
        click.echo(f"   时长: {rec.duration:.1f}秒")
    else:
        click.echo(f"❌ 录制不存在: {recording_id}")


@rec.command("add-action")
@click.argument("recording_id")
@click.option("--type", "-t", required=True, help="动作类型: click, type, hotkey, wait, launch_app")
@click.option("--x", type=int, help="X坐标")
@click.option("--y", type=int, help="Y坐标")
@click.option("--text", help="文本输入")
@click.option("--key", help="热键")
@click.option("--app", help="应用名称")
def rec_add_action(recording_id, type, x, y, text, key, app):
    """手动添加动作"""
    converter = create_screen_recorder(str(DATA_DIR))
    
    action_data = {
        "action_type": type,
        "timestamp": time.time()
    }
    
    if x is not None:
        action_data["x"] = x
    if y is not None:
        action_data["y"] = y
    if text:
        action_data["text"] = text
    if key:
        action_data["key"] = key
    if app:
        action_data["app"] = app
    
    if converter.add_action(recording_id, action_data):
        click.echo(f"✅ 已添加动作")
    else:
        click.echo(f"❌ 录制不存在")


@rec.command("list")
def rec_list():
    """列出录制"""
    converter = create_screen_recorder(str(DATA_DIR))
    recordings = converter.list_recordings()
    
    if not recordings:
        click.echo("暂无录制")
        return
    
    click.echo(f"\n🎬 录制列表 ({len(recordings)}个):\n")
    for rec in recordings:
        click.echo(f"  {rec.recording_id}: {rec.name}")
        click.echo(f"    动作: {len(rec.actions)}, 时长: {rec.duration:.1f}秒")
        click.echo(f"    创建: {datetime.fromtimestamp(rec.created_at).strftime('%Y-%m-%d %H:%M')}")


@rec.command("convert")
@click.argument("recording_id")
@click.option("--name", "-n", help="工作流名称")
@click.option("--mode", "-m", default="image", help="检测模式: image, text, coordinate")
def rec_convert(recording_id, name, mode):
    """转换为工作流"""
    converter = create_screen_recorder(str(DATA_DIR))
    
    from src.screen_recorder import ElementDetection
    detection = ElementDetection.TEXT if mode == "text" else ElementDetection.COORDINATE if mode == "coordinate" else ElementDetection.IMAGE
    
    result = converter.convert_to_workflow(recording_id, name, detection)
    
    if result:
        click.echo(f"\n✅ 转换成功: {result.workflow_name}")
        click.echo(f"   工作流ID: {result.workflow_id}")
        click.echo(f"   步骤数: {len(result.steps)}")
        
        if result.warnings:
            click.echo(f"\n⚠️ 警告 ({len(result.warnings)}个):")
            for w in result.warnings[:3]:
                click.echo(f"   - {w}")
        
        # 保存工作流
        workflow_file = DATA_DIR / f"workflow_{result.workflow_id}.json"
        with open(workflow_file, "w", encoding="utf-8") as f:
            f.write(converter.export_workflow_json(result))
        
        click.echo(f"\n💾 工作流已保存: {workflow_file}")
    else:
        click.echo(f"❌ 录制不存在或为空")


@rec.command("analyze")
@click.argument("recording_id")
def rec_analyze(recording_id):
    """分析录制"""
    converter = create_screen_recorder(str(DATA_DIR))
    analysis = converter.analyze_recording(recording_id)
    
    if not analysis:
        click.echo(f"❌ 录制不存在: {recording_id}")
        return
    
    click.echo(f"\n📊 录制分析: {analysis.get('name')}")
    click.echo(f"  动作数: {analysis.get('action_count')}")
    click.echo(f"  时长: {analysis.get('duration', 0):.1f}秒")
    click.echo(f"  分辨率: {analysis.get('resolution')}")
    
    if analysis.get('action_types'):
        click.echo(f"\n  动作类型分布:")
        for at, count in analysis['action_types'].items():
            click.echo(f"    - {at}: {count}")


import time
import datetime


if __name__ == "__main__":
    cli()
