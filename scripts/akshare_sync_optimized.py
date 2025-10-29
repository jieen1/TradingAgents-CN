#!/usr/bin/env python3
"""
AKShare 优化同步脚本

优化点：
1. 预先缓存股票列表，避免重复获取
2. 批量处理，显示详细进度
3. 失败重试机制
4. 详细的错误日志

使用方法：
    python scripts/akshare_sync_optimized.py
    python scripts/akshare_sync_optimized.py --batch-size 100  # 调整批次大小
    python scripts/akshare_sync_optimized.py --delay 0.2  # 调整延迟时间
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from motor.motor_asyncio import AsyncIOMotorClient
from app.core.config import settings
from tradingagents.dataflows.providers.china.akshare import AKShareProvider
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


async def sync_stock_basic_info(
    batch_size: int = 100,
    delay: float = 0.3,
    retry_failed: bool = True
):
    """
    同步股票基础信息
    
    Args:
        batch_size: 批次大小
        delay: 每只股票之间的延迟（秒）
        retry_failed: 是否重试失败的股票
    """
    logger.info("=" * 80)
    logger.info("🚀 AKShare 优化同步股票基础信息")
    logger.info("=" * 80)
    
    # 1. 连接数据库
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB]
    collection = db["stock_basic_info"]
    
    # 2. 初始化 Provider
    provider = AKShareProvider()
    await provider.connect()
    
    try:
        # 3. 获取股票列表
        logger.info("📋 获取股票列表...")
        stock_list = await provider.get_stock_list()
        
        if not stock_list:
            logger.error("❌ 未获取到股票列表")
            return
        
        total_count = len(stock_list)
        logger.info(f"✅ 获取到 {total_count} 只股票")
        
        # 4. 预加载股票列表缓存（用于降级查询）
        logger.info("🔄 预加载股票列表缓存...")
        await provider._get_stock_list_cached()
        
        # 5. 批量处理
        logger.info(f"\n🔄 开始同步...")
        logger.info(f"   批次大小: {batch_size}")
        logger.info(f"   延迟时间: {delay}秒/股票")
        logger.info("")
        
        success_count = 0
        failed_count = 0
        failed_stocks = []
        
        start_time = datetime.now()
        
        for i, stock in enumerate(stock_list, 1):
            code = stock.get("code")
            name = stock.get("name", "")
            
            if not code:
                logger.warning(f"⚠️  [{i}/{total_count}] 跳过: 缺少股票代码")
                failed_count += 1
                continue
            
            try:
                # 获取详细信息
                basic_info = await provider.get_stock_basic_info(code)
                
                if basic_info:
                    # 添加 symbol 字段（向后兼容）
                    basic_info["symbol"] = code
                    basic_info["updated_at"] = datetime.utcnow()
                    
                    # 更新数据库
                    await collection.update_one(
                        {"code": code},
                        {"$set": basic_info},
                        upsert=True
                    )
                    
                    # 显示进度（每10只股票显示一次）
                    if i % 10 == 0 or i == total_count:
                        logger.info(f"📈 [{i}/{total_count}] {code} ({basic_info.get('name', 'N/A')}) "
                                   f"- 行业: {basic_info.get('industry', 'N/A')}")
                    
                    success_count += 1
                else:
                    logger.warning(f"⚠️  [{i}/{total_count}] {code} 获取失败")
                    failed_count += 1
                    failed_stocks.append({"code": code, "name": name})
                
                # 延迟，避免API限流
                if i < total_count:
                    await asyncio.sleep(delay)
                
                # 每批次输出统计
                if i % batch_size == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    speed = i / elapsed if elapsed > 0 else 0
                    eta = (total_count - i) / speed if speed > 0 else 0
                    
                    logger.info(f"\n📊 进度统计:")
                    logger.info(f"   已处理: {i}/{total_count} ({i*100//total_count}%)")
                    logger.info(f"   成功: {success_count}, 失败: {failed_count}")
                    logger.info(f"   速度: {speed:.1f} 只/秒")
                    logger.info(f"   预计剩余时间: {eta/60:.1f} 分钟\n")
                
            except Exception as e:
                logger.error(f"❌ [{i}/{total_count}] {code} 处理异常: {e}")
                failed_count += 1
                failed_stocks.append({"code": code, "name": name, "error": str(e)})
        
        # 6. 输出最终统计
        elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("📊 同步完成统计")
        logger.info("=" * 80)
        logger.info(f"   总计: {total_count} 只股票")
        logger.info(f"   成功: {success_count} 只")
        logger.info(f"   失败: {failed_count} 只")
        logger.info(f"   成功率: {success_count*100//total_count if total_count > 0 else 0}%")
        logger.info(f"   总耗时: {elapsed/60:.1f} 分钟")
        logger.info(f"   平均速度: {success_count/elapsed if elapsed > 0 else 0:.1f} 只/秒")
        logger.info("=" * 80)
        
        # 7. 重试失败的股票
        if retry_failed and failed_stocks:
            logger.info(f"\n🔄 重试失败的 {len(failed_stocks)} 只股票...")
            
            retry_success = 0
            for i, stock in enumerate(failed_stocks, 1):
                code = stock["code"]
                try:
                    logger.info(f"   [{i}/{len(failed_stocks)}] 重试 {code}...")
                    basic_info = await provider.get_stock_basic_info(code)

                    if basic_info and basic_info.get("name").startswith('股票'):
                        basic_info['name'] = stock["name"]

                    if basic_info:
                        basic_info["symbol"] = code
                        basic_info["updated_at"] = datetime.utcnow()
                        
                        await collection.update_one(
                            {"code": code},
                            {"$set": basic_info},
                            upsert=True
                        )
                        
                        logger.info(f"      ✅ 成功: {basic_info.get('name', 'N/A')}")
                        retry_success += 1
                    else:
                        logger.warning(f"      ❌ 仍然失败")
                    
                    await asyncio.sleep(delay * 2)  # 重试时延迟加倍
                    
                except Exception as e:
                    logger.error(f"      ❌ 异常: {e}")
            
            logger.info(f"\n📊 重试结果: 成功 {retry_success}/{len(failed_stocks)}")
        
        # 8. 保存失败列表
        if failed_stocks:
            failed_file = project_root / "failed_stocks_akshare.txt"
            with open(failed_file, 'w', encoding='utf-8') as f:
                for stock in failed_stocks:
                    f.write(f"{stock['code']}\t{stock.get('name', 'N/A')}\t{stock.get('error', '')}\n")
            logger.info(f"\n💾 失败列表已保存到: {failed_file}")
        
        logger.info("")
        logger.info("✅ 同步完成！")
        
    finally:
        client.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="AKShare 优化同步股票基础信息",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="批次大小（默认：100）"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.3,
        help="每只股票之间的延迟（秒）（默认：0.3）"
    )
    parser.add_argument(
        "--no-retry",
        action="store_true",
        help="不重试失败的股票"
    )
    
    args = parser.parse_args()
    
    asyncio.run(sync_stock_basic_info(
        batch_size=args.batch_size,
        delay=args.delay,
        retry_failed=not args.no_retry
    ))


if __name__ == "__main__":
    main()

