# ETKN 补丁生成器与优化补丁

本目录存放**生成 ETKN bind-mount 补丁**所需的源材料。运行时补丁本体不进仓库（体积大、随上游变化），
只保存「如何从上游原版生成补丁」的确定性配方。

| 文件 | 作用 |
| --- | --- |
| `repository_optimizations.diff` | 订阅列表性能优化（`dm_by_item` / `se_flags` / `season_keys` 三套临时表 + `media_has_current_version`）。unified diff，`--label repository.py`，**`patch -p0`** 直接可用 |
| `gen_p115_organize_patch.py` | 整理输出压缩（`rejected` 由 84MB 压到 0.37MB），防 `cannot allocate memory for output buffer` |
| `gen_center_control_patch.py` | 中心控制退避 120s→3s、长连接 `recv` 不再 15s 中断、断线诊断 |
| `gen_play_pool_patch.py` | 小号播放池跨 PlaySessionId 复用临时副本（复播 3.2s → 0.04s） |
| `gen_screenshots_patch.py` | 截图范围 `capture_item_ids_for_library()` 加进程内 TTL 缓存 + 后台异步刷新 |
| `gen_virtual_library_patch.py` | 「新建媒体库」弹窗筛选项：`_local_options()` 全表 823MB JSON 扫描（27.0s）改 SQL jsonb 聚合（1.37s）+ TTL 缓存；TMDB 类型表并行请求 + `/config` 落盘快照（冷启动免外网） |

## 生成本地补丁

```bash
cd /vol1/1000/docker/etkn
python3 gen_play_pool_patch.py        # 读 play_pool.py.orig → 写 play_pool_patch.py(+.md5)
python3 gen_screenshots_patch.py     # 同上
python3 gen_virtual_library_patch.py # 同上（读 virtual_library_service.py.orig）
python3 auto_rebase_patch.py --all   # 镜像更新后统一 rebase 8 个补丁
```

`auto_rebase_patch.py` 的 `rebase_repository()` 现在分两步：先 `optimize_library_status_counts()`
（内联字符串改造），再 `apply_repository_optimizations()`（套用本目录 diff 并校验标记）。
任一步失败即抛错，**不再静默产出缺优化的版本** —— 2026-10-24/25 两次「rebase 冲掉优化、订阅列表从 1s 退回 37s」即由此根除。
