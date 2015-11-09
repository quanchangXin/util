package com.yzg365.common.dao;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.yzg365.common.annotation.Column;
import com.yzg365.common.annotation.Extendable;
import com.yzg365.common.annotation.ItemClass;
import com.yzg365.common.annotation.Key;
import com.yzg365.common.annotation.PrimaryKey;
import com.yzg365.common.helper.DefaultTableNameProvider;
import com.yzg365.common.helper.ITableName;
import com.yzg365.common.helper.ITableNameProvider;
import com.yzg365.common.util.StringTools;
import com.yzg365.common.util.exception.YzgRuntimeException;

/**
 * Dao的基类，提供基本，公用的数据库访问方法。CryptBaseDao 继承这个类提供加密的数据访问方法
 * 说明：Vo 默认情况下字段名和数据库列名一致，不一致时使用Column注解获取列名
 * 数据库默认主键为seqId(bigint 自增)
 * 提供类似dbutil的相对原始的数据封装
 * 提供基于对象的增删改查操作
 * 支持一对多关系结果的封装
 * @author seven
 *
 */
@SuppressWarnings("unchecked")
public abstract class JdbcBaseDao extends JdbcDaoSupport {

	protected static final Logger logger = LoggerFactory.getLogger(JdbcBaseDao.class);

	

	public JdbcBaseDao2() {
		super();
		logger.debug(this.getClass().getName() + " 初始化");
	}

	private ITableNameProvider iTableNameProvider = null;

	protected ITableNameProvider getTableNameProvider() {
		if (iTableNameProvider == null) {
			iTableNameProvider = new DefaultTableNameProvider();
		}
		return iTableNameProvider;
	}

	protected void setTableNameProvider(ITableNameProvider provider) {
		this.iTableNameProvider = provider;
	}
	
	/**
	 * 获取dao对象的表名
	 * 
	 * @author beta
	 */
	public String getTableNameByClazz(Class<?> clazz) {
		getTableNameProvider().setDaoClass(clazz);
		return getTableNameProvider().getTableName();
	}

	/**
	 * 将sql中的?替换为实际的参数，用于日志打印和数据恢复，实际不执行
	 * 
	 * @param sql
	 * @param params
	 * @return
	 */
	private String getLogSql(String sql, Object[] params) {
		for (int i = 0; i < params.length; i++) {
			// 如果字符串类型添加 '',其它类型将?替换为对应的值
			if(params[i] == null){
				sql = sql.replaceFirst("\\?", "NULL");
			}
			else if (params[i].getClass() == String.class) {
				sql = sql.replaceFirst("\\?", "'" + params[i].toString() + "'");
			}
			else {
				sql = sql.replaceFirst("\\?", params[i].toString());
			}
		}
		return sql;
	}

	private String getPkColumnName(Class clazz) throws Exception {
		String pkColumnName;
		Field field = clazz.getDeclaredField("seqId");
		Column column = field.getAnnotation(Column.class);
		pkColumnName = field.getName();
		if (column != null) {
			pkColumnName = column.columnName();
		}
		return pkColumnName;
	}

	/**
	 * 构造查询条件，如果val为null返回空字符串
	 * 
	 * @param columnName
	 *            数据库列名
	 * @param cond
	 *            条件		支持  >, <, >=, <=, =, like, in_str, in_int 
	 *            			eg 	in val 为 String(逗号隔开) 或 List 
	 *            				columnName IN(?,?,?)  paramList[...,Obj1,Obj2,Obj3]
	 * @param val
	 *            条件值
	 * @param paramList
	 *            参数列表 包含多个val
	 * @return
	 */
	public String sqlCondBuilder(String columnName, String cond, Object val, List<Object> paramList) {
		if (columnName == null || cond == null || val == null || paramList == null)
			return "";

		if ("like".equalsIgnoreCase(cond)) {
			paramList.add("%" + val + "%");
		}
		//字段包含字符串
		else if(cond.equalsIgnoreCase("contains")){
			paramList.add(val);
			return " AND INSTR("+columnName+", ?) ";
		}
		//字段 在集合中，改为 ? 代替 防止sql注入
		else if(cond.equalsIgnoreCase("in")){
			StringBuilder inBuilder = new StringBuilder(" AND "+columnName+" IN(");
			String instr;
			List params = new ArrayList<Object>();
			if(val instanceof String){
				String paramStr = (String) val;
				String[] paramArr = paramStr.split(",");
				for(int i = 0; i < paramArr.length; i++){
					params.add(paramArr[i]);
				}
			}else if(val instanceof List){
				params = (List<Object>) val;
			}
			
			for(Object obj : params){
				inBuilder.append("?,");
			}
			
			paramList.addAll(params);
			//去除最后一个逗号
			instr = inBuilder.substring(0, inBuilder.length()-1);
			return instr+") ";
		}
		else {
			paramList.add(val);
		}

		return " AND " + columnName + " " + cond + " ? ";
	}

	/**
	 * 根据某一sql语句返回记录列表，只返回第一列
	 */
	public <T> List<T> queryToList(String sql, Object[] params) {
		sql = sql.trim();
		final List<T> datas = new ArrayList<T>();
		getJdbcTemplate().query(sql, params, new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				datas.add((T) rs.getObject(1));
			}
		});
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + datas.size() + "]");
		return datas;
	}

	/**
	 * 根据某一sql语句返回记录列表
	 */
	public List<Object[]> queryToListArray(String sql, Object[] params) {
		sql = sql.trim();
		final List<Object[]> list = new ArrayList<Object[]>();
		getJdbcTemplate().query(sql, params, new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				ResultSetMetaData rsMetaData = rs.getMetaData();
				int cnt = rsMetaData.getColumnCount();
				Object[] array = new Object[cnt];

				for (int i = 0; i < cnt; i++) {
					array[i] = rs.getObject(i + 1);
				}
				list.add(array);
			}
		});
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + list.size() + "]");
		return list;
	}

	/**
	 * 根据某一sql语句返回记录列表
	 */
	public List<Map<String, Object>> queryToListMap(String sql, Object[] params) {
		final List<Map<String, Object>> listMap = new ArrayList<Map<String, Object>>();
		getJdbcTemplate().query(sql, params, new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				ResultSetMetaData rsMetaData = rs.getMetaData();
				Map<String, Object> map = new HashMap<String, Object>();
				int cnt = rsMetaData.getColumnCount();
				for (int i = 1; i <= cnt; i++) {
					map.put(rsMetaData.getColumnLabel(i), rs.getObject(i));
				}
				listMap.add(map);
			}
		});
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + listMap.size() + "]");
		return listMap;
	}

	/**
	 * 查询返回单个Vo 调用 public <T> List<T> queryToList(String sql, Object[] params,
	 * final Class clazz) 实现
	 * 
	 * @param sql
	 * @param params
	 * @param clazz
	 * @return
	 */
	public <T> T query(String sql, Object[] params, final Class clazz) {
		List<T> list = queryToList(sql, params, clazz);
		return list.size() > 0 ? list.get(0) : null;
	}

	/**
	 * 根据某一sql语句返回记录列表,并封装成Vo 如果数据库的列名和Vo的属性名不一致 将其重命名为 属性名即可
	 * 可以查询多张表，并将其封装到一个对象中
	 */
	public <T> List<T> queryToList(String sql, Object[] params, final Class clazz) {
		final List<T> list = new ArrayList<T>();
		getJdbcTemplate().query(sql, params, new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				Map<String, String> columnMap = this.getColumnMap(clazz);
				T t;
				try {
					t = (T) clazz.newInstance();
					ResultSetMetaData rsMetaData = rs.getMetaData();
					int cnt = rsMetaData.getColumnCount();
					Field field;
					String fieldName;
					Object fieldObject;
					for (int i = 1; i <= cnt; i++) {
						fieldName = rsMetaData.getColumnLabel(i);
						fieldObject = rs.getObject(i);
						// field = clazz.getDeclaredField(fieldName);
						if (fieldObject == null) {
							continue;
						}
						// 如果是column,则替换成fieldName
						if (columnMap.containsKey(fieldName)) {
							fieldName = columnMap.get(fieldName);
						}
						BeanUtils.setProperty(t, fieldName, fieldObject);
					}
					list.add(t);
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}

			private Map<String, String> getColumnMap(final Class clazz) {
				Field[] fields = clazz.getDeclaredFields();
				Map<String, String> map = new HashMap<String, String>();
				for (Field f : fields) {
					Column column = f.getAnnotation(Column.class);
					if (column != null) {
						map.put(column.columnName(), f.getName());
					}
				}
				return map;
			}
		});
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + list.size() + "]");
		return list == null ? Collections.<T> emptyList() : list;
	}

	public <T> T query(String sql, Object[] params, Class clazz, String agreKey) {
		List<T> list = queryToList(sql, params, clazz, agreKey);
		return list.size() > 0 ? list.get(0) : null;
	}

	/**
	 * 查询适用于一对多关系， Vo中有list类型的属性 list属性上添加注解@ItemClass(clazz=XXX.class)
	 * clazz=XXX.class表示List中元素的类型为XXX Vo和XXX为标准的java bean 
	 * eg. 
	 * SELECT	staffId,itemId AS `items.itemId`,num AS `items.num` 
	 * FROM		idb_hr.salary_record 
	 * WHERE 	balanceMonth = '2014-07' 
	 * Vo中存在staffId
	 * items 属性，其中items是List类型items可以是任意其它名字
	 * `items.itemId`对应的列将设置List（如items）中的一个元素的itemId字段的值 `items.num`设置num字段
	 *  sql返回的每条记录将封装一个List中的元素
	 * 
	 * sql 结果为
	 * staffId	`items.itemId` `items.num` 
	 * HR001	baseSalary	2000
	 * HR001	attendanceBonus 1000
	 * HR002	baseSalary	2500
	 * HR002	attendanceBonus 1200
	 * Vo
	 * class StaffSalary{
	 * 		private String staffId;
	 * 		@ItemClass(clazz=SalaryItem.class)
	 * 		private List<SalaryItem> items;
	 * 	.......
	 * }
	 * 
	 * class SalaryItem{
	 * 		private String itemId;
	 * 		private BigDecimal num;
	 * ........
	 * }
	 * 封装结果为 [
	 * 				{"staffId":"HR001","items":[{"itemId":"baseSalary","num":2000},{"itemId":"attendanceBonus","num":1000}]},
	 * 				{"staffId":"HR002","items":[{"itemId":"baseSalary","num":2500},{"itemId":"attendanceBonus","num":1200}]}
	 * 			]
	 * 
	 * @param sql
	 *            sql
	 * @param params
	 *            替代sql中的?
	 * @param clazz
	 *            Vo 类型
	 * @param agreKey
	 *            Vo 的唯一标识，目前只对应表中的一列
	 * @return
	 */
	public <T> List<T> queryToList(String sql, Object[] params, Class clazz, String agreKey) {
		List<Map<String, Object>> list = queryToListMap(sql, params);
		List<T> datas = new ArrayList<T>();
		// String lastAgreKey = null;
		Object lastAgreKey = null;
		T t = null;
		List items = null;
		ItemClass itemClass = null;
		Class itemClazz = null;
		Object bean;
		Object oldPropVal;
		try {
			for (Map<String, Object> m : list) {
				// 新的Vo对象t
				Object curAgreKey = m.get(agreKey);
				if (lastAgreKey == null || !curAgreKey.equals(lastAgreKey)) {
					t = (T) clazz.newInstance();
					datas.add(t);
					lastAgreKey = curAgreKey;
					items = null;
				}

				// 对一个Vo对象赋值
				Object item = null;
				for (Entry<String, Object> entry : m.entrySet()) {
					String propName = entry.getKey();
					Object propVal = entry.getValue();
					String[] propties = propName.split("\\.");
					Field field = clazz.getDeclaredField(propties[0]);
					if (propties.length > 1) {
						if (items == null) {
							items = new ArrayList();
							BeanUtils.setProperty(t, propties[0], items);
						}
						if (itemClazz == null) {
							itemClass = field.getAnnotation(ItemClass.class);
							itemClazz = itemClass.clazz();
						}
						if (item == null) {
							item = itemClazz.newInstance();
							items.add(item);
						}
						field = itemClazz.getDeclaredField(propties[1]);
						bean = item;
						propName = propties[1];
					}
					else {
						bean = t;
						propName = propties[0];
					}

					oldPropVal = BeanUtils.getProperty(bean, propName);
					// 如果已经赋值，或者是数字且不是初始值0，那么跳过
					if (oldPropVal != null && !oldPropVal.toString().equals("0")) {
						continue;
					}
					BeanUtils.setProperty(bean, propName, propVal);
				}
			}
		}
		catch (Exception ex) {
			throw new YzgRuntimeException(ex);
		}
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + list.size() + "]");
		return datas;
	}

	public <T> T queryToFlat(String sql, Object[] params, Class clazz, String agreKey) {
		List<T> list = queryToFlatList(sql, params, clazz, agreKey);
		return list.size() > 0 ? list.get(0) : null;
	}

	/**
	 * 将一对多关系的多方转换为Vo的一个属性值 eg
	 * sql 结果为
	 * staffId	propName propVal	
	 * HR001	baseSalary	2000
	 * HR001	attendanceBonus 1000
	 * HR002	baseSalary	2500
	 * HR002	attendanceBonus 1200
	 * Vo
	 * class StaffSalary{
	 * 		private String staffId;
	 * 		private BigDecimal baseSalary;
	 * 		private	BigDecimal attendanceBonus;
	 * 	.......
	 * }
	 * 封装结果为 [
	 * 				{"staffId":"HR001","baseSalary":2000,"attendanceBonus":1000,},
	 * 				{"staffId":"HR002","baseSalary":2500,"attendanceBonus":1200,}
	 * 			]
	 * @param sql
	 * @param params
	 * @param clazz		vo 对象类型
	 * @param agreKey	有相同列值(agreKey)的结果行的特定列(propName propVal)将被封装到一个vo中的不同字段(propName)中
	 *            
	 * @return
	 */
	public <T> List<T> queryToFlatList(String sql, Object[] params, Class clazz, String agreKey) {
		List<Map<String, Object>> list = queryToListMap(sql, params);
		List<T> datas = new ArrayList<T>();
		String lastAgreKey = null;
		T t = null;
		String propName = null;
		Object propVal = null;
		Object oldPropVal = null;
		try {
			for (Map<String, Object> m : list) {
				// 新的Vo对象t
				String curAgreKey = (String) m.get(agreKey);
				if (lastAgreKey == null || !curAgreKey.equals(lastAgreKey)) {
					t = (T) clazz.newInstance();
					datas.add(t);
					lastAgreKey = curAgreKey;
				}
				for (Entry<String, Object> entry : m.entrySet()) {
					String columLable = entry.getKey();
					Object columVal = entry.getValue();
					if ("propVal".equals(columLable)) {
						continue;
					}
					else if ("propName".equals(columLable)) {
						propName = (String) m.get(columLable);
						propVal = m.get("propVal");
					}
					else {
						propName = columLable;
						propVal = columVal;
					}
					oldPropVal = BeanUtils.getProperty(t, propName);
					if (oldPropVal == null) {
						BeanUtils.setProperty(t, propName, propVal);
					}
				}

			}
		}
		catch (Exception ex) {
			throw new YzgRuntimeException(ex);
		}
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + list.size() + "]");
		return datas;
	}

	/**
	 * 跟public <T> List<T> queryToFlatList(String sql, Object[] params, Class clazz, String agreKey)
	 * 相同的作用，只是结果保存在map中
	 * @param sql
	 * @param params
	 * @param agreKey
	 * @return
	 */
	public List<Map<String, Object>> queryToFlatMapList(String sql, Object[] params,String agreKey) {
		List<Map<String, Object>> list = queryToListMap(sql, params);
		List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
		String lastAgreKey = null;
		Map<String, Object> map = null;
		String propName = null;
		Object propVal = null;
		try {
			for (Map<String, Object> m : list) {
				// 新的Vo对象t
				String curAgreKey = (String) m.get(agreKey);
				if (lastAgreKey == null || !curAgreKey.equals(lastAgreKey)) {
					map = new HashMap<String, Object>();
					datas.add(map);
					lastAgreKey = curAgreKey;
				}
				for (Entry<String, Object> entry : m.entrySet()) {
					String columLable = entry.getKey();
					Object columVal = entry.getValue();
					if ("propVal".equals(columLable)) {
						continue;
					}
					//获取列名 和列只
					else if ("propName".equals(columLable)) {
						propName = (String) m.get(columLable);
						propVal = m.get("propVal");
					}
					//共同的部分
					else {
						propName = columLable;
						propVal = columVal;
					}

					map.put(propName, propVal);
				}

			}
		}
		catch (Exception ex) {
			throw new YzgRuntimeException(ex);
		}
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + list.size() + "]");
		return datas;
	}
	
	public Map<String, Object> queryToFlatMap(String sql, Object[] params,String agreKey){
		List<Map<String, Object>> datas = queryToFlatMapList(sql, params, agreKey);
		return datas.size() == 0 ? null : datas.get(0);
	}
	
	/**
	 * 返回单个值，使用与任何类型
	 * 
	 * @param sql
	 * @param params
	 * @return
	 */
	public <T> T getSingleValue(String sql, Object[] params) {
		sql = sql.trim();
		final List<T> list = new ArrayList<T>();
		getJdbcTemplate().query(sql, params, new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				try {
					T t = (T) rs.getObject(1);
					list.add(t);
				}
				catch (Exception e) {
					throw new YzgRuntimeException();
				}
			}
		});
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + list.size() + "]");
		return list.size() > 0 ? list.get(0) : null;
	}

	/**
	 * 执行曾 删 改 操作
	 * 
	 * @param sql
	 * @param params
	 * @return
	 */
	public int update(String sql, Object[] params) {
		int ret = getJdbcTemplate().update(sql, params);
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + ret + "]");
		return ret;
	}

	/**
	 * 基本的增删改查操作，针对一条记录
	 */
	/**
	 * 如果有Column注解，通过注解获取数据库列名， 否则对应的数据库字段为属性名， 插入所有非空字段 ，标记为@PrimaryKey和@Extendable的字段不插入
	 * 
	 * @param t
	 * @param pwd
	 * @return
	 */
	public <T> T add(T t) {
		Class<?> clazz = t.getClass();
		String tableName = getTableName(t);
		StringBuilder sqlColumn = new StringBuilder("(");
		StringBuilder sqlValue = new StringBuilder("(");
		List params = new ArrayList();

		String fname;
		Method method;
		Object fieldObject;
		int ret = -1;
		try {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				fname = field.getName();
				method = clazz.getDeclaredMethod("get" + capitalize(fname));
				fieldObject = method.invoke(t);

				// 不处理null
				if (fieldObject == null) {
					continue;
				}

				if (field.getAnnotation(Extendable.class) != null) {
//					logger.debug(fname + "标记为@Extendable，不保存此字段");
					continue;
				}

				// 对于seqid默认为自增主键
				if (field.getAnnotation(PrimaryKey.class) != null) {
//					logger.debug(fname + "为自增主键，不用插入");
					continue;
				}

				// 通过注解获取属性对应的数据库字段名,主要用于两者不一致的表
				Column column = field.getAnnotation(Column.class);
				if (column != null) {
					fname = column.columnName();
				}

				// 默认属性名和数据库字段名一致
				sqlColumn.append(getEacapeFieldName(fname) + ",");
				sqlValue.append("?,");

				params.add(fieldObject);
			}

			String sqlInsert = "insert into " + tableName
					+ sqlColumn.substring(0, sqlColumn.length() - 1) + ") values"
					+ sqlValue.substring(0, sqlValue.length() - 1) + ")";
			// 执行sql
			ret = this.update(sqlInsert, params.toArray());
			// 如果需要回填主键，要通过查询获得，之前的实现是查询出的主键可能是错误的 TODO
		}
		catch (Exception ex) {
			logger.error(ex.getMessage());
			throw new YzgRuntimeException(ex);
		}
		return ret == 1 ? t : null;
	}

	/**
	 * 如果主键存在根据主键删除 如果主键为null 或0 则根据@key 删除
	 * 
	 * @param t
	 * @return
	 */
	public <T> T delete(T t) {
		int ret = -1;
		T old = get(t);
		try {
			Class<?> clazz = t.getClass();
			String tableName = getTableName(t);
			Method method;
			Object seqId = null;
			try {
				method = clazz.getDeclaredMethod("getSeqId");
				seqId = method.invoke(t);
			} catch (Exception e) {
				logger.debug("没有seqId");
				e.printStackTrace();
			}
			
			StringBuilder sql = new StringBuilder(" DELETE FROM " + tableName + " WHERE 1=1 ");
			List params = new ArrayList();
			Field[] fields = clazz.getDeclaredFields();
			String fieldname;
			Object fieldObject;

			// 如果主键存在
			if (seqId != null && (Long) seqId != 0) {
				sql.append(" AND " + getPkColumnName(clazz) + " = ? ");
				params.add(seqId);
			}
			else {// 否则根据@key 删除
				for (Field field : fields) {
					fieldname = field.getName();
					method = clazz.getDeclaredMethod("get" + capitalize(fieldname));
					fieldObject = method.invoke(t);
					Object fieldValue = method.invoke(t);
					if (field.getAnnotation(Key.class) != null && fieldObject != null) {
						Column column = field.getAnnotation(Column.class);
						if (column != null) {
							fieldname = column.columnName();
						}
						sql.append(" AND " + fieldname + "= ? ");
						params.add(fieldObject);
					}
				}
			}

			ret = update(sql.toString(), params.toArray());
		}
		catch (Exception ex) {
			logger.error(ex.getMessage());
			throw new YzgRuntimeException(ex);
		}

		return ret == 1 ? old : null;
	}

	/**
	 * TODO 如果主键存在，首先根据主键查询出对象，比较是否有变化(ts insertTime 除外)
	 * 如果有变化再更新，目前没有比较是否变化，直接更新 根据主键或者添加了@Key注解的字段更新，null 字段不更新 ，有注解 @Key @Extendable @PrimaryKey
	 * 的字段不更新 当有多个字段@Key都能够确定一条记录是只需要设置一个字段即可，多个也没有问题
	 * 
	 * @param t
	 * @return
	 */
	public <T> T update(T t) {
		int ret = -1;
		try {
			Class<?> clazz = t.getClass();
			String tableName = getTableName(t);
			StringBuilder update = new StringBuilder(" UPDATE " + tableName + " SET ");
			StringBuilder where = new StringBuilder(" WHERE 1=1 ");
			List fieldParams = new ArrayList();
			List condParams = new ArrayList();
			String fieldname;
			Object fieldObject;
			Field[] fields = clazz.getDeclaredFields();
			Object seqId = null;
			Method method = null;
			boolean seqidExist = false;
			for (Field field : fields) {
				PrimaryKey primaryKey = field.getAnnotation(PrimaryKey.class);
				if (primaryKey != null && !seqidExist) {
					method = clazz.getDeclaredMethod("get" + capitalize(field.getName()));
					seqId = method.invoke(t);
					if (seqId != null && (Long) seqId != 0) {
						seqidExist = true;
						where.append(" AND " + getPkColumnName(clazz) + " = ? ");
						condParams.add(seqId);
						break;
					}
				}
			}

			// if (seqId != null && (Long) seqId != 0) {
			// seqidExist = true;
			// where.append(" AND " + getPkColumnName(clazz) + " = ? ");
			// condParams.add(seqId);
			// }

			for (Field field : fields) {
				fieldname = field.getName();
				method = clazz.getDeclaredMethod("get" + capitalize(fieldname));
				fieldObject = method.invoke(t);

				if (fieldObject == null) {
					continue;
				}

				Column column = field.getAnnotation(Column.class);
				if (column != null) {
					fieldname = column.columnName();
				}

				// 如果没有根据主键更新，那么根据有注解@key且 field不为null的字段更新
				if (!seqidExist && field.getAnnotation(Key.class) != null) {
					where.append(" AND " + fieldname + "= ? ");
					condParams.add(fieldObject);
					continue;
				}
				if (field.getAnnotation(Extendable.class) != null) {
					continue;
				}
				if (field.getAnnotation(PrimaryKey.class) != null) {
					continue;
				}

				// 要更新的字段
				update.append(fieldname + "= ?,");
				fieldParams.add(fieldObject);
			}
			String sql = update.substring(0, update.length() - 1) + where;
			fieldParams.addAll(condParams);
			ret = update(sql, fieldParams.toArray());
		}
		catch (Exception e) {
			throw new YzgRuntimeException(e.getMessage());
		}

		return ret == 1 ? t : null;
	}

	/**
	 * 首先根据主键查询 如果主键为null 或0 则根据t中添加了@Key注解的属性查询 返回单个对象
	 * 
	 * @param t
	 * @return
	 */
	public <T> T get(T t) {
		try {
			Class clazz = t.getClass();
			Field[] fields = clazz.getDeclaredFields();
			Method primaryKeyMethod = null;
			Object primaryKey = null;
			for (Field field : fields) {
				if (field.getAnnotation(PrimaryKey.class) != null) {
					String expectedColumnName = getColumnName(field);
					primaryKeyMethod = clazz.getDeclaredMethod("get"
							+ StringTools.firstCharToUpperCase(expectedColumnName));
					primaryKey = primaryKeyMethod.invoke(t);
				}
			}
			List params = new ArrayList();
			Map<String, String> column2Field = new HashMap<String, String>();
			String tableName = this.getTableName(t, clazz);
			StringBuilder sql = new StringBuilder("SELECT * FROM " + tableName + " WHERE 1=1 ");
			// 如果主键存在
			if (primaryKey != null && (Long) primaryKey != 0) {
				sql.append(" AND " + getPkColumnName(clazz) + " = ? ");
				params.add(primaryKey);
			}
			else {// 否则
				for (Field field : fields) {
					String fname = field.getName();
					Method method = clazz.getDeclaredMethod("get" + capitalize(fname));
					Object fieldValue = method.invoke(t);
					Column column = field.getAnnotation(Column.class);
					// 根据标记为Key的所有字段查询
					if (field.getAnnotation(Key.class) != null && fieldValue != null) {
						if (column != null) {
							fname = column.columnName();
						}
						sql.append(" AND " + fname + "= ? ");
						params.add(fieldValue);
					}

				}
			}

			for (Field field : fields) {
				String fname = this.getColumnName(field);
				column2Field.put(fname, field.getName());
			}

			// 执行查询并封装结果
			List<Map<String, Object>> list = queryToListMap(sql.toString(), params.toArray());
			t = (T) clazz.newInstance();

			for (Map<String, Object> map : list) {
				for (Entry<String, Object> entry : map.entrySet()) {
					String columnName = entry.getKey();
					Object fieldObject = entry.getValue();
//					logger.debug("columnName="+columnName);
					if (fieldObject == null) {
						continue;
					}
					//首先判断属性是否存在
					try {
						String propertyValue = BeanUtils.getProperty(t, column2Field.get(columnName));
					} catch (Exception e) {
						logger.debug("do not have such property");
						e.printStackTrace();
					}
					BeanUtils.setProperty(t, column2Field.get(columnName), fieldObject);
				}
			}

			return list.size() == 0 ? null : t;
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new YzgRuntimeException(e);
		}
	}

	/**
	 * 批量更新操作
	 * @param sql
	 * @param batchArgs
	 * @return
	 */
	public int[] batchUpdate(String sql, final List<Object[]> batchArgs) {
		int[] retArray = getJdbcTemplate().batchUpdate(sql, new BatchPreparedStatementSetter() {

			@Override
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				Object[] values = batchArgs.get(i);
				for (int j = 1; j <= values.length; j++) {
					ps.setObject(j, values[j - 1]);
				}
			}

			@Override
			public int getBatchSize() {
				return batchArgs.size();
			}
		});

		for (int i = 0; i < batchArgs.size(); i++) {
			logger.debug("[sql] " + getLogSql(sql, batchArgs.get(i)) + "[" + retArray[i] + "]");
		}

		return retArray;
	}

	public <T> int[] addList(final List<T> list, Class clazz) {
		if (null == list || 0 == list.size()) {
			return new int[0];
		}

		getTableNameProvider().setDaoClass(clazz);
		String tableName = getTableNameProvider().getTableName();
		StringBuilder sqlColumn = new StringBuilder("(");
		StringBuilder sqlValue = new StringBuilder("(");

		T t = list.get(0);
		String fname;
		Method method;
		Object fieldObject;
		String sqlInsert;
		/**
		 * 生成sql
		 */
		try {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				fname = field.getName();
				method = clazz.getDeclaredMethod("get" + capitalize(fname));
				fieldObject = method.invoke(t);

				// 不处理null
				if (fieldObject == null) {
					continue;
				}

				if (field.getAnnotation(Extendable.class) != null) {
					logger.debug(fname + "标记为@Extendable，不保存此字段");
					continue;
				}

				// 对于seqid默认为自增主键
				if (field.getAnnotation(PrimaryKey.class) != null) {
					logger.debug(fname + "为自增主键，不用插入");
					continue;
				}

				// 通过注解获取属性对应的数据库字段名,主要用于两者不一致的表
				Column column = field.getAnnotation(Column.class);
				if (column != null) {
					fname = column.columnName();
				}

				// 默认属性名和数据库字段名一致
				sqlColumn.append(getEacapeFieldName(fname) + ",");
				sqlValue.append("?,");
			}

			sqlInsert = "insert into " + tableName + sqlColumn.substring(0, sqlColumn.length() - 1)
					+ ") values" + sqlValue.substring(0, sqlValue.length() - 1) + ")";
		}
		catch (Exception ex) {
			logger.error(ex.getMessage());
			throw new YzgRuntimeException(ex);
		}

		/**
		 * 生成参数列表
		 */
		List<Object[]> batchArgs = new ArrayList<Object[]>();
		for (int i = 0; i < list.size(); i++) {
			t = list.get(i);
			List params = new ArrayList();
			try {
				Field[] fields = clazz.getDeclaredFields();
				for (Field field : fields) {
					fname = field.getName();
					method = clazz.getDeclaredMethod("get" + capitalize(fname));
					fieldObject = method.invoke(t);

					// 不处理null
					if (fieldObject == null) {
						continue;
					}

					if (field.getAnnotation(Extendable.class) != null) {
						logger.debug(fname + "标记为@Extendable，不保存此字段");
						continue;
					}

					// 对于seqid默认为自增主键
					if (field.getAnnotation(PrimaryKey.class) != null) {
						logger.debug(fname + "为自增主键，不用插入");
						continue;
					}

					params.add(fieldObject);
				}
			}
			catch (Exception ex) {
				logger.error(ex.getMessage());
				throw new YzgRuntimeException(ex);
			}
			batchArgs.add(params.toArray());
		}

		return batchUpdate(sqlInsert, batchArgs);
	}

	/**
	 * @param t
	 * @param clazz
	 * @return
	 */
	private <T> String getTableName(T t, Class clazz) {
		String tableName;
		if (t instanceof ITableName) {
			tableName = ((ITableName) t).getTableName();
		}
		else {
			getTableNameProvider().setDaoClass(clazz);
			tableName = getTableNameProvider().getTableName();
		}
		return tableName;
	}

	/**
	 * @param field
	 */
	private String getColumnName(Field field) {
		String expectedColumnName = StringTools.EMPTY;
		Column column = field.getAnnotation(Column.class);
		if (column != null)
			expectedColumnName = column.columnName();
		else
			expectedColumnName = field.getName();
		return expectedColumnName;
	}

	/**
	 * 根据某一sql语句返回记录Map对象
	 */

	public <K, V> Map<K, V> queryToMap(String sql, Object[] params) {
		final Map<K, V> map = new HashMap<K, V>();
		getJdbcTemplate().query(sql, params, new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				map.put((K) rs.getObject(1), (V) rs.getObject(2));
			}
		});
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + map.size() + "]");
		return map;
	}

	// public <K, V> Map<K, V> queryToMap(String sql, Object[] params) {
	// return queryToMap(sql, params, null, null);
	// }

	public <K, V> Map<K, V> queryToMap(String sql, Object[] params, K key, V value) {
		sql = sql.trim();
		final Map<K, V> datas = new LinkedHashMap<K, V>();
		if (key != null && value != null)
			datas.put(key, value);
		getJdbcTemplate().query(sql, params, new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				datas.put((K) rs.getObject(1), (V) rs.getObject(2));
			}
		});
		logger.debug("[sql] " + getLogSql(sql, params) + "[" + datas.size() + "]");
		return datas;
	}

	public int getSingleInt(String sql, Object[] params) {
		try {
			int ret = super.getJdbcTemplate().queryForInt(sql, params);
			logger.debug("[sql] " + getLogSql(sql, params) + "[1]");
			return ret;
		}
		catch (DataAccessException e) {
			// don't care the exception
			return 0;
		}
	}

	public long getSingleLong(String sql, Object[] params) {
		try {
			long ret = super.getJdbcTemplate().queryForLong(sql, params);
			logger.debug("[sql] " + getLogSql(sql, params) + "[1]");
			return ret;
		}
		catch (DataAccessException e) {
			// don't care the exception
			return 0L;
		}
	}

	/**
	 * 执行某一sql语句，返回一double数据
	 */
	public double getSingleDouble(String sql, Object[] params) {
		try {
			sql = sql.trim();
			Double d = (Double) getJdbcTemplate().queryForObject(sql, params, Double.class);
			logger.debug("[sql] " + getLogSql(sql, params) + "[1]");
			if (null != d) {
				return d.doubleValue();
			}
			else {
				return 0D;
			}
		}
		catch (DataAccessException e) {
			// don't care the exception
			return 0D;
		}
	}

	/**
	 * 执行某一sql语句，返回一字符串数据
	 */
	public String getSingleString(String sql, Object[] params) {
		try {
			sql = sql.trim();
			String data = (String) getJdbcTemplate().queryForObject(sql, params, String.class);
			logger.debug("[sql] " + getLogSql(sql, params) + "[1]");
			if (StringTools.isEmpty(data)) {
				data = "";
			}
			return data;
		}
		catch (DataAccessException e) {
			// don't care the exception
			return "";
		}
	}

	private static String capitalize(String name) {
		return name.substring(0, 1).toUpperCase() + name.substring(1);
	}

	/**
	 * 此方法用于Spring2.5自动扫描时注入数据源用。如果需要手动设置数据源时调用
	 * {@link #setDataSource(DataSource)}方法。 如果数据源名称不为dataSource的话，请重写这个方法。
	 * 
	 * @param dataSource
	 *            数据源
	 */
	@Resource(name = "dataSource")
	public void setDataS(DataSource dataSource) {
		this.setDataSource(dataSource);
		logger.debug("在" + this.getClass().getName() + ",dataSource被注入");
	}

	/**
	 * 判断指定类型是否被生成sql时所支持
	 * 
	 * @param type
	 * @return
	 */
	public boolean isSupportType(Class type) {
		if (type == String.class) {
			return true;
		}
		if (isNumType(type)) {
			return true;
		}
		if (type == Date.class) {
			return true;
		}
		return false;
	}

	/**
	 * 判断类似是否是数值型
	 * 
	 * @param type
	 * @return
	 */
	public boolean isNumType(Class<?> type) {
		if (type == Short.TYPE || type == Short.class) {
			return true;
		}
		if (type == Integer.TYPE || type == Integer.class) {
			return true;
		}
		if (type == Long.TYPE || type == Long.class) {
			return true;
		}
		if (type == Float.TYPE || type == Float.class) {
			return true;
		}
		if (type == Double.TYPE || type == Double.class) {
			return true;
		}
		if (type.getName().equals("java.math.BigDecimal")) {
			return true;
		}
		return false;
	}

	/**
	 * 获得主键注解对象实体 即使对应的vo没有注解也会返回一个默认的注解（seqid,auto_increament）
	 * 
	 * @param clazz
	 * @return
	 */
	private PrimaryKey getPrimayKey(Class<?> clazz) {
		PrimaryKey pk = null;
		Field[] fields = clazz.getDeclaredFields();
		for (Field f : fields) {
			pk = f.getAnnotation(PrimaryKey.class);
			if (pk != null) {
				break;
			}
		}
		return pk;
	}

	private String getEacapeFieldName(String fieldName) {
		return "`" + fieldName + "`";
	}
	
	private <T> String getTableName(T t) {
		if (t instanceof ITableName) {
			return ((ITableName) t).getTableName();
		}
		else {
			getTableNameProvider().setDaoClass(t.getClass());
			return getTableNameProvider().getTableName();
		}
	}

}
