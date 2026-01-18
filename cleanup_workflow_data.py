#!/usr/bin/env python3
"""
清理工作流产生的中间数据（Redis + CouchDB）

使用方法：
---------
python3 cleanup_workflow_data.py [--dry-run] [--redis-only] [--couchdb-only]

选项：
------
--dry-run: 仅显示将要删除的数据，不实际删除
--redis-only: 仅清理 Redis
--couchdb-only: 仅清理 CouchDB
"""
import os
import sys
import fnmatch
import argparse

# 延迟导入，如果不存在则友好提示
try:
    import redis
except ImportError:
    redis = None

try:
    import couchdb
except ImportError:
    couchdb = None


def cleanup_redis(redis_host, redis_port, dry_run=False):
    """清理 Redis 中的工作流相关 key"""
    print("\n[INFO] === Cleaning Redis ===" + (" (DRY RUN)" if dry_run else ""))
    
    if redis is None:
        print("[ERROR] redis-py module not installed. Run: pip install redis")
        return False
    
    try:
        client = redis.StrictRedis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        client.ping()
        
        # 获取所有 key
        all_keys = client.keys('*')
        print(f"[INFO] Total keys in Redis: {len(all_keys)}")
        
        # 工作流相关的 key 模式
        workflow_patterns = [
            'req-*', 'warmup-*', 'sys-*', 'const_target_*',
            '*video*', '*recognizer*', '*svd*', '*wordcount*',
            '*split*', '*transcode*', '*merge*', '*upload*',
            '*adult*', '*violence*', '*extract*', '*censor*',
            '*translate*', '*mosaic*', '*compute*', '*count*',
            '*start*', '*res*', '*matrix*', '*file*', '*img*',
            '*text*', '*illegal*', '*transcoded*', '*splited*',
            '*mosaic_image*', '*final_*'
        ]
        
        keys_to_delete = []
        for key in all_keys:
            for pattern in workflow_patterns:
                if fnmatch.fnmatch(key, pattern):
                    keys_to_delete.append(key)
                    break
        
        print(f"[INFO] Found {len(keys_to_delete)} workflow-related keys")
        
        if keys_to_delete:
            if dry_run:
                print(f"[DRY-RUN] Would delete the following keys (showing first 20):")
                for key in keys_to_delete[:20]:
                    print(f"  - {key}")
                if len(keys_to_delete) > 20:
                    print(f"  ... and {len(keys_to_delete) - 20} more")
            else:
                # 批量删除（每次最多 1000 个，避免阻塞）
                batch_size = 1000
                deleted_total = 0
                for i in range(0, len(keys_to_delete), batch_size):
                    batch = keys_to_delete[i:i+batch_size]
                    deleted = client.delete(*batch)
                    deleted_total += deleted
                    print(f"[INFO] Deleted batch {i//batch_size + 1}: {deleted} keys")
                print(f"[INFO] Total deleted from Redis: {deleted_total} keys")
        else:
            print("[INFO] No workflow keys to delete")
        
        # 显示清理后的状态
        remaining = client.dbsize()
        print(f"[INFO] Remaining keys in Redis: {remaining}")
        
    except Exception as e:
        print(f"[ERROR] Failed to cleanup Redis: {e}")
        return False
    
    return True


def cleanup_couchdb(couchdb_url, dry_run=False):
    """清理 CouchDB faas_data 数据库"""
    print("\n[INFO] === Cleaning CouchDB ===" + (" (DRY RUN)" if dry_run else ""))
    
    if couchdb is None:
        print("[ERROR] CouchDB module not installed. Run: pip install couchdb")
        return False
    
    try:
        server = couchdb.Server(couchdb_url)
        
        if 'faas_data' not in server:
            print("[INFO] CouchDB faas_data database does not exist")
            return True
        
        db = server['faas_data']
        
        # 收集所有文档
        docs_to_delete = []
        doc_sizes = []
        
        for doc_id in db:
            if not doc_id.startswith('_'):  # 跳过设计文档
                doc = db[doc_id]
                doc_size = doc.get('size', 0)
                doc_sizes.append(doc_size)
                docs_to_delete.append({'_id': doc_id, '_rev': doc['_rev'], '_deleted': True})
        
        print(f"[INFO] Found {len(docs_to_delete)} documents in faas_data database")
        
        if docs_to_delete:
            total_size = sum(doc_sizes)
            print(f"[INFO] Total data size: {total_size / (1024*1024):.2f} MB")
            
            if dry_run:
                print(f"[DRY-RUN] Would delete {len(docs_to_delete)} documents")
                if doc_sizes:
                    print(f"[DRY-RUN] Size range: {min(doc_sizes)} - {max(doc_sizes)} bytes")
                    print(f"[DRY-RUN] Average size: {sum(doc_sizes)/len(doc_sizes):.0f} bytes")
            else:
                # 批量删除
                db.update(docs_to_delete)
                print(f"[INFO] Deleted {len(docs_to_delete)} documents from CouchDB")
                
                # 触发压缩以回收磁盘空间
                print("[INFO] Triggering database compaction...")
                try:
                    db.compact()
                    print("[INFO] Compaction started (runs in background)")
                except Exception as e:
                    print(f"[WARN] Failed to trigger compaction: {e}")
        else:
            print("[INFO] No documents to delete")
        
    except Exception as e:
        print(f"[ERROR] Failed to cleanup CouchDB: {e}")
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="清理工作流产生的中间数据（Redis + CouchDB）",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--dry-run', action='store_true', help='仅显示将要删除的数据，不实际删除')
    parser.add_argument('--redis-only', action='store_true', help='仅清理 Redis')
    parser.add_argument('--couchdb-only', action='store_true', help='仅清理 CouchDB')
    parser.add_argument('--redis-host', default='172.17.0.1', help='Redis 主机地址')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis 端口')
    parser.add_argument('--couchdb-url', default='http://openwhisk:openwhisk@172.17.0.1:5984/',
                        help='CouchDB URL')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("工作流中间数据清理工具")
    print("=" * 60)
    
    if args.dry_run:
        print("\n⚠️  DRY RUN 模式：仅显示将要删除的内容，不会实际删除")
    
    success = True
    
    # 清理 Redis
    if not args.couchdb_only:
        success &= cleanup_redis(args.redis_host, args.redis_port, args.dry_run)
    
    # 清理 CouchDB
    if not args.redis_only:
        success &= cleanup_couchdb(args.couchdb_url, args.dry_run)
    
    print("\n" + "=" * 60)
    if success:
        if args.dry_run:
            print("✅ DRY RUN 完成")
            print("\n提示: 移除 --dry-run 参数可执行实际清理")
        else:
            print("✅ 清理完成")
            print("\n建议:")
            print("  - 检查 CouchDB 数据目录大小是否减小")
            print("  - 如需立即回收磁盘空间，可在 CouchDB 容器内运行压缩")
    else:
        print("❌ 清理过程中出现错误")
        sys.exit(1)


if __name__ == '__main__':
    main()
