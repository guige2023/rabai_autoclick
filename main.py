import sys
import os

os.environ['FLAGS_use_mkldnn'] = '0'
os.environ['FLAGS_enable_onednn_backend'] = '0'
os.environ['FLAGS_allocator_strategy'] = 'naive_best_fit'

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from PyQt5.QtWidgets import QApplication
from ui.main_window import MainWindow


def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    app.setApplicationName('RabAI AutoClick')
    app.setApplicationVersion('1.0.0')
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
