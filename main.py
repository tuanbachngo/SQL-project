from paddleocr import PaddleOCR

ocr = PaddleOCR(lang="vi", use_angle_cls=True, show_log=False)
res = ocr.ocr("test.png", cls=True)

text = "\n".join([x[1][0] for x in res[0]])
print(text)
