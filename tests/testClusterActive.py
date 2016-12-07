from medusa.medusasystem import set_running_clusters, get_running_clusters


def test_running_cluster():
    """
    print running clusters
    """
    set_running_clusters()
    print get_running_clusters()

    # time.sleep(10)

if __name__ == "__main__":
    print "cluster working"
    test_running_cluster()
