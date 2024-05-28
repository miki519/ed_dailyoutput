from sqlalchemy import create_engine, select, case, func, literal_column ,between
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from datetime import timedelta, date ,datetime

class Base(DeclarativeBase):
	# type_annotation_map = {
	#     date: datetime.date,
	# }
	pass

class PurchaseOrder(Base):
	__tablename__ = 'purchase_order'
	 
	id: Mapped[int] = mapped_column(primary_key=True)
	number: Mapped[str]
	pay_method: Mapped[int]
	status: Mapped[int]
	pay_status: Mapped[int]
	created: Mapped[date]
	fee: Mapped[int]
	
	def __repr__(self) -> str:
		return f"PurchaseOrder(id={self.id!r}, 訂單編號={self.number!r}, 付款方式={self.pay_method!r}, 訂單狀態={self.status!r}, 付款狀態={self.pay_status!r}, 建立日期={self.created!r}, 訂單金額={self.fee!r})"

class PurchaseOrderProduct(Base):
	__tablename__ = 'purchase_order_product'

	id: Mapped[int] = mapped_column(primary_key=True)
	purchase_order_id: Mapped[int]
	product_name: Mapped[str]
	fee: Mapped[int]
	
	def __repr__(self) -> str:
		return f"PurchaseOrderProduct(id={self.id!r}, purchhase_oreder_id={self.purchase_order_id!r}, 商品名稱={self.product_name!r}, 商品金額={self.fee!r})"

class PurchaseOrderProductItem(Base):
	__tablename__ = 'purchase_order_product_item'
	
	id: Mapped[int] = mapped_column(primary_key=True)
	purchase_order_product_id: Mapped[int]
	product_item_id: Mapped[int]
	quantity: Mapped[int]
	
	def __repr__(self) -> str:
		return f"PurchaseOrderProductItem(id={self.id!r}, pruchase_ordder_prodduct_id={self.purchase_order_product_id!r}, 商品數量={self.quantity!r})"

class Product_item(Base):
	__tablename__ = 'product_item'
	
	id: Mapped[int] = mapped_column(primary_key=True)
	abbreviation: Mapped[str]
	

class N_Orders(Base):
	__tablename__ ='Orders'
	
	id: Mapped[str] = mapped_column(primary_key=True)
	orderNo: Mapped[str]
	status: Mapped[int]
	deliveryStatus: Mapped[int]
	paymentStatus: Mapped[int]
	totalPrice: Mapped[int]
	deliveryType: Mapped[int]
	paymentMethod: Mapped[int]
	createdAt: Mapped[date]

class N_OrderDetails(Base):
	__tablename__ = 'OrderDetails'
	
	id: Mapped[int] = mapped_column(primary_key=True)
	orderId: Mapped[str]
	quantity: Mapped[int]
	price: Mapped[int]

class N_OrderGoods(Base):
	__tablename__ = 'OrderGoods'
	
	id: Mapped[int] = mapped_column(primary_key=True)
	orderDetailId: Mapped[int]
	goodsId: Mapped[int]

class N_Goods(Base):
	__tablename__ = 'Goods'

	id: Mapped[int] = mapped_column(primary_key=True)
	name: Mapped[str]
	
class QuerySet_N:
	def __init__(self,host, user, password, ssl, database):
		self.ssl_ca = {'ssl_ca': f'{ssl}'}
		self.engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}', connect_args= self.ssl_ca)
	
	def _website(self):
		# else_='OTHER'
		website_mapping = [('BG', 'BuyGoodLife')]
		
		website = [(func.left(N_Orders.orderNo, func.length(N_Orders.orderNo) - 14) == prefix, vaule) for prefix, vaule in website_mapping]
		return case(*website, else_='OTHER').label("網站")
	
	def _status(self):
		status_mapping = [
			('2', '已出貨'),
			('7', '拒收'),
			('8', '取消訂單'),
			('5', '訂單失敗'),
			('0', '新訂單'),
			('413', '已退換貨')
		]
		return case(*[(N_Orders.status == key, value) for key, value in status_mapping], else_='其他狀態-需查詢').label('訂單狀態')
	
	def _pay_method(self):
		# else_='其他付款方式-需查詢'
		pay_method_mapping = [('1', '貨到付款'),('2', '信用卡付款')]
		return case(*[(N_Orders.paymentMethod == key, value) for key, value in pay_method_mapping],else_='其他付款方式-需查詢').label('付款方式')
	
	def _pay_status(self):
		# else_='其他付款狀態-需查詢'
		pay_status_mapping = [('205', '信用卡付款失敗'),('200', '未付款'),('202', '已付款')]
		return case(*[(N_Orders.paymentStatus == key, value) for key, value in pay_status_mapping], else_='其他付款狀態-需查詢').label('付款狀態')

	def _order_products(self):
		return func.group_concat(N_Goods.name,'/',
		N_OrderDetails.quantity,'/',N_OrderDetails.price,).label('訂購商品')
			
	def query(self, start_date, end_date):
		if start_date and end_date:
			self.start_date = start_date
			self.end_date = end_date
		else:
			self.start_date = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
			self.end_date = datetime.combine(date.today() - timedelta(days=1), datetime.max.time())

		result = select(
			self._website(),
			self._status(),
			self._pay_method(),
			self._pay_status(),
			N_Orders.totalPrice.label('訂單金額'),
			self._order_products(),
			N_Orders.createdAt,        
		).join(
			N_OrderDetails, N_OrderDetails.orderId == N_Orders.id
		).join(
			N_OrderGoods, N_OrderGoods.orderDetailId == N_OrderDetails.id
		).join(
			N_Goods, N_Goods.id == N_OrderGoods.goodsId
		).group_by(
			N_Orders.orderNo,
			N_Orders.totalPrice,
			N_Orders.paymentMethod,
			N_Orders.status,
			N_Orders.paymentStatus,
			N_Orders.createdAt
		).where(
			between(N_Orders.createdAt,self.start_date,self.end_date)
		)
		return result
	
	
	
class QuerySet_O:
	def __init__(self,host, user, password, ssl, database):
		self.ssl_ca = {'ssl_ca': f'{ssl}'}
		self.engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}', connect_args= self.ssl_ca)
	
	def _website(self):
		# else_='OTHER'
		website_mapping = [('DM', 'DailyMore'),('IB', 'InnerBB'),('LLTW', '一點TW'),('LL', '一點COM'),('PI', 'popideal')]
		
		website = [(func.left(PurchaseOrder.number, func.length(PurchaseOrder.number) - 14) == prefix, vaule) for prefix, vaule in website_mapping]
		return case(*website, else_='OTHER').label("網站")
	
	def _status(self):
		status_mapping = [
			('100', '新訂單'),('101', '訂單確認中'),('102', '待出貨'),('103', '已出貨'),('104', '已到貨'),('105', '已開立發票'),('106', '缺貨中'),('107', '訂單轉移'),('108', '開立異常'),('199', '上傳失敗'),('200', '退貨確認中'),('201', '退貨確認'),('202', '退貨確認'),('203', '已退貨'),('204', '拒收'),('205', '退款中'),('206', '已退款'),('299', '上傳失敗'),('300', '換貨確認中'),('301', '換貨確認'),('302','換貨中'),('303','已換貨'),('399','上傳失敗'),('400','已取消'),('401','延遲出貨'),('402','取消確認中'),('500', '交易完成'),('600', '換貨單') 
		]
		return case(*[(PurchaseOrder.status == key, value) for key, value in status_mapping], else_='其他狀態-需查詢').label('訂單狀態')
	
	def _pay_method(self):
		# else_='其他付款方式-需查詢'
		pay_method_mapping = [('0', '貨到付款'),('1', '信用卡付款'),('3', '未知付款方式'),('4','先享後付 AFTEE')]
		return case(*[(PurchaseOrder.pay_method == key, value) for key, value in pay_method_mapping],else_='其他付款方式-需查詢').label('付款方式')
	
	def _pay_status(self):
		# else_='其他付款狀態-需查詢'
		pay_status_mapping = [('0', '未付款'),('1', '付款確認中'),('2', '已付款'),('3', '待刷退'),('4', '已刷退')]
		return case(*[(PurchaseOrder.pay_status == key, value) for key, value in pay_status_mapping], else_='其他付款狀態-需查詢').label('付款狀態')

	def _order_products(self):
		return func.group_concat(Product_item.abbreviation,'/',
		PurchaseOrderProductItem.quantity,'/',PurchaseOrderProduct.fee,).label('訂購商品')
			
	def query(self, start_date, end_date):
		if start_date and end_date:
			self.start_date = start_date
			self.end_date = end_date
		else:
			self.start_date = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
			self.end_date = datetime.combine(date.today() - timedelta(days=1), datetime.max.time())

		result = select(
			self._website(),
			self._status(),
			self._pay_method(),       
			self._pay_status(),
			PurchaseOrder.fee.label('訂單金額'),
			self._order_products(),
			PurchaseOrder.created,        
		).join(
			PurchaseOrderProduct, PurchaseOrder.id == PurchaseOrderProduct.purchase_order_id
		).join(
			PurchaseOrderProductItem, PurchaseOrderProduct.id == PurchaseOrderProductItem.purchase_order_product_id
		).join(
			Product_item, Product_item.id == PurchaseOrderProductItem.product_item_id
		
		).group_by(
			PurchaseOrder.number,
			PurchaseOrder.fee,
			PurchaseOrder.pay_method,
			PurchaseOrder.status,
			PurchaseOrder.pay_status,
			PurchaseOrder.created
		).where(
			between(PurchaseOrder.created,self.start_date,self.end_date)
		)
		return result