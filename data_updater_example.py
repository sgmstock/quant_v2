"""
DataUpdater 使用示例

演示如何正确使用数据更新器
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_management import DatabaseManager, DataUpdater




def test_data_updater():
    """测试数据更新器"""
    print("=== 测试 DataUpdater ===")
    
    try:
        # 创建数据库管理器
        db_manager = DatabaseManager("databases/quant_system.db")
        print("✅ 数据库管理器创建成功")
        
        # 创建数据更新器
        data_updater = DataUpdater(db_manager)
        print("✅ 数据更新器创建成功")
        
        # 获取当前数据状态
        summary = db_manager.get_data_summary("k_daily")
        print(f"📊 当前数据状态: {summary}")
        
        # 测试从CSV更新数据（如果文件存在）
        csv_path = "databases/daily_update_converted.csv"
        if os.path.exists(csv_path):
            print(f"📁 找到CSV文件: {csv_path}")
            print("🔄 开始从CSV更新数据...")
            success = data_updater.update_daily_data_from_csv(csv_path)
            print(f"结果: {'成功' if success else '失败'}")
        else:
            print(f"⚠️ CSV文件不存在: {csv_path}")
            print("💡 提示: 可以手动创建CSV文件进行测试")
        
        # 测试AkShare更新（小范围测试）
        print("\n🔄 测试AkShare数据更新...")
        test_stocks = ["000001", "000002"]  # 只测试2个股票
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        
        print(f"📈 测试股票: {test_stocks}")
        print(f"📅 测试日期: {start_date} 到 {end_date}")
        
        # 注意：这里只是演示，实际运行可能需要网络连接
        print("💡 提示: AkShare更新需要网络连接，这里只演示接口")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_timeframe_conversion():
    """测试时间周期转换"""
    print("\n=== 测试时间周期转换 ===")
    
    try:
        from data_management import TimeframeConverter
        
        # 创建数据库管理器
        db_manager = DatabaseManager("databases/quant_system.db")
        
        # 创建时间周期转换器
        converter = TimeframeConverter(db_manager)
        print("✅ 时间周期转换器创建成功")
        
        # 获取一些股票进行测试
        stocks = db_manager.get_stock_list("k_daily")
        if stocks:
            test_stocks = stocks[:2]  # 只测试前2个股票
            print(f"📈 测试股票: {test_stocks}")
            
            # 测试周线转换（不实际执行，只检查方法）
            print("💡 提示: 时间周期转换功能已就绪")
            print("💡 提示: 实际转换需要大量时间，这里只演示接口")
        
        return True
        
    except Exception as e:
        print(f"❌ 时间周期转换测试失败: {e}")
        return False

def main():
    """主函数"""
    print("🚀 DataUpdater 使用示例\n")
    
    # 运行测试
    tests = [
        test_data_updater,
        test_timeframe_conversion
    ]
    
    success_count = 0
    for test in tests:
        if test():
            success_count += 1
    
    print(f"\n=== 测试结果汇总 ===")
    print(f"成功测试: {success_count}/{len(tests)}")
    
    if success_count == len(tests):
        print("🎉 所有测试成功！")
    else:
        print("⚠️ 部分测试失败！")

if __name__ == "__main__":
    # main()
    db_manager = DatabaseManager("databases/quant_system.db")
    data_updater = DataUpdater(db_manager)
    # 【首选】聚宽数据CSV文件路径
    JQDATA_CSV_PATH = "databases/daily_update_last.csv"
    # 【转换后】聚宽数据转换后的文件路径
    # JQDATA_CONVERTED_PATH = "databases/daily_update_converted.csv"
    # 【备用】Akshare缓存文件路径
    # AKSHARE_CACHE_PATH = 'databases/akshare_daily.csv'
    # data_updater.run()
    data_updater.check_recent_data(stock_code='000029', days=5)
