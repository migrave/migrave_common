from sensor_msgs.msg import Image
from cv_bridge import CvBridge, CvBridgeError

def get_cv_image(image, image_encoding="bgr8"):
    cvbridge = CvBridge()
    try:
        cv_image = cvbridge.imgmsg_to_cv2(image, image_encoding)
    except CvBridgeError as e:
        print(e)
        return

    return cv_image

