from fennel.lib.ascii_visualizer import draw_graph


def test_simple_graph():
    vertices = ["1", "2", "3", "4", "5"]
    edges = [("1", "2"), ("2", "3"), ("3", "4"), ("4", "5"), ("5", "1")]
    draw_graph(vertices, edges)
