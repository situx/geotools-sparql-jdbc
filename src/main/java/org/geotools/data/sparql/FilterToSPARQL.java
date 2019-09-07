package org.geotools.data.sparql;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.Date;
import java.util.logging.Level;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.FilterCapabilities;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JoinPropertyName;
import org.geotools.util.Converters;
import org.geotools.util.factory.GeoTools;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.And;
import org.opengis.filter.BinaryLogicOperator;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.Id;
import org.opengis.filter.IncludeFilter;
import org.opengis.filter.Or;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.Add;
import org.opengis.filter.expression.Divide;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.Multiply;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.expression.Subtract;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.DistanceBufferOperator;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.temporal.Instant;

public class FilterToSPARQL extends FilterToSQL {
	
	public FilterToSPARQL() {
		this.out=new StringWriter();
	}
	
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, Object extraData) {
        // basic checks
        if (filter == null) throw new NullPointerException("Filter to be encoded cannot be null");
        if (!(filter instanceof BinarySpatialOperator))
            throw new IllegalArgumentException(
                    "This filter is not a binary spatial operator, "
                            + "can't do SDO relate against it: "
                            + filter.getClass());

        // extract the property name and the geometry literal
        BinarySpatialOperator op = (BinarySpatialOperator) filter;
        Expression e1 = op.getExpression1();
        Expression e2 = op.getExpression2();

        if (e1 instanceof Literal && e2 instanceof PropertyName) {
            e1 = (PropertyName) op.getExpression2();
            e2 = (Literal) op.getExpression1();
        }

        if (e1 instanceof PropertyName) {
            // handle native srid
            currentGeometry = null;
            currentSRID = null;
            currentDimension = null;
            if (featureType != null) {
                // going thru evaluate ensures we get the proper result even if the
                // name has
                // not been specified (convention -> the default geometry)
                AttributeDescriptor descriptor = (AttributeDescriptor) e1.evaluate(featureType);
                if (descriptor instanceof GeometryDescriptor) {
                    currentGeometry = (GeometryDescriptor) descriptor;
                    currentSRID =
                            (Integer) descriptor.getUserData().get(JDBCDataStore.JDBC_NATIVE_SRID);
                    currentDimension =
                            (Integer) descriptor.getUserData().get(Hints.COORDINATE_DIMENSION);
                }
            }
        }

        if (e1 instanceof PropertyName && e2 instanceof Literal) {
            // call the "regular" method
            return visitBinarySpatialOperator(
                    filter,
                    (PropertyName) e1,
                    (Literal) e2,
                    filter.getExpression1() instanceof Literal,
                    extraData);
        } else {
            // call the join version
            return visitBinarySpatialOperator(filter, e1, e2, extraData);
        }
    }
    
    @Override
    protected void visitLiteralGeometry(Literal expression) throws IOException {
        Geometry g = (Geometry) evaluateLiteral(expression, Geometry.class);
        if (g instanceof LinearRing) {
            // WKT does not support linear rings
            g = g.getFactory().createLineString(((LinearRing) g).getCoordinateSequence());
        }
        if(currentSRID!=null) {
        	out.write("\"<http://www.opengis.net/def/crs/OGC/1.3/"+currentSRID+">" + g.toText() + "\"^^geo:wktLiteral");
        }else {
        	out.write("\"" + g.toText() + "\"^^geo:wktLiteral");
        }
    }
    
    protected Object visitBinarySpatialOperator(
            BinarySpatialOperator filter, Expression e1, Expression e2, Object extraData) {
            return visitBinarySpatialOperator(filter, e1, e2, false, extraData);
    }
    
    /**
     * Writes out a non null, non geometry literal. The base class properly handles null, numeric
     * and booleans (true|false), and turns everything else into a string. Subclasses are expected
     * to override this shall they need a different treatment (e.g. for dates)
     *
     * @param literal
     * @throws IOException
     */
    protected void writeLiteral(Object literal) throws IOException {
        if (literal == null) {
            out.write("NULL");
        } else if (literal instanceof Number || literal instanceof Boolean) {
            out.write(String.valueOf(literal));
        } else if (literal instanceof java.sql.Date || literal instanceof java.sql.Timestamp) {
            // java.sql.date toString declares to always format to yyyy-mm-dd
            // (and TimeStamp uses a similar approach)
            out.write("'" + literal + "'");
        } else if (literal instanceof java.util.Date) {
            // get back to the previous case
            Timestamp ts = new java.sql.Timestamp(((Date) literal).getTime());
            out.write("'" + ts + "'");
        } else if (literal instanceof Instant) {
            java.util.Date date = ((Instant) literal).getPosition().getDate();
            Timestamp ts = new java.sql.Timestamp(date.getTime());
            out.write("'" + ts + "'");
        } else if (literal.getClass().isArray()) {
            // write as a SQL99 array
            out.write("ARRAY[");
            int length = Array.getLength(literal);
            for (int i = 0; i < length; i++) {
                writeLiteral(Array.get(literal, i));
                if (i < length - 1) {
                    out.write(", ");
                }
            }
            out.write("]");
        } else {
            // we don't know the type...just convert back to a string
            String encoding = (String) Converters.convert(literal, String.class, null);
            if (encoding == null) {
                // could not convert back to string, use original l value
                encoding = "?"+literal.toString();
            }

            // sigle quotes must be escaped to have a valid sql string
            String escaped = encoding.replaceAll("'", "''");
            out.write("'" + escaped + "'");
        }
    }
    
    /**
     * Writes the SQL for the attribute Expression.
     *
     * @param expression the attribute to turn to SQL.
     * @throws RuntimeException for io exception with writer
     */
    public Object visit(PropertyName expression, Object extraData) throws RuntimeException {
        LOGGER.finer("exporting PropertyName");

        Class target = null;
        if (extraData instanceof Class) {
            target = (Class) extraData;
        }

        try {
            SimpleFeatureType featureType = this.featureType;

            // check for join
            if (expression instanceof JoinPropertyName) {
                // encode the prefix
                out.write(escapeName(((JoinPropertyName) expression).getAlias()));
                out.write(".");
            }

            // first evaluate expression against feautre type get the attribute,
            //  this handles xpath
            AttributeDescriptor attribute = null;
            try {
                attribute = (AttributeDescriptor) expression.evaluate(featureType);
            } catch (Exception e) {
                // just log and fall back on just encoding propertyName straight up
                String msg = "Error occured mapping " + expression + " to feature type";
                LOGGER.log(Level.WARNING, msg, e);
            }
            String encodedField;
            if (attribute != null) {
                encodedField = fieldEncoder.encode(escapeName(attribute.getLocalName()));
                if (target != null && target.isAssignableFrom(attribute.getType().getBinding())) {
                    // no need for casting, it's already the right type
                    target = null;
                }
            } else {
                // fall back to just encoding the property name
                encodedField = fieldEncoder.encode(escapeName(expression.getPropertyName()));
            }

            // handle destination type if necessary
            if (target != null) {
                out.write(cast(encodedField, target));
            } else {
                out.write(encodedField);
            }

        } catch (java.io.IOException ioe) {
            throw new RuntimeException("IO problems writing attribute exp", ioe);
        }
        return "?"+extraData;
    }

    /**
     * Surrounds a name with the SQL escape string.
     *
     * <p>If the name contains the SQL escape string, the SQL escape string is duplicated.
     *
     * @param name
     */
    public String escapeName(String name) {
    	return "?"+name;
    }
    
    protected Object visitBinarySpatialOperator(
            BinarySpatialOperator filter,
            Expression e1,
            Expression e2,
            boolean swapped,
            Object extraData) {

        try {

            /*if (!(filter instanceof Disjoint)) {
                out.write("MbrIntersects(");
                e1.accept(this, extraData);
                out.write(",");
                e2.accept(this, extraData);
                out.write(")");

                if (!(filter instanceof BBOX)) {
                    out.write(" && ");
                }
            }*/

            if (filter instanceof BBOX) {
                // nothing to do. already encoded above
                return extraData;
            }

            if (filter instanceof DistanceBufferOperator) {
                out.write("geo:geof:Distance(");
                e1.accept(this, extraData);
                out.write(", ");
                e2.accept(this, extraData);
                out.write(")");

                if (filter instanceof DWithin) {
                    out.write("<");
                } else if (filter instanceof Beyond) {
                    out.write(">");
                } else {
                    throw new RuntimeException("Unknown distance operator");
                }
                out.write(Double.toString(((DistanceBufferOperator) filter).getDistance()));
            } else if (!(filter instanceof BBOX)) {
                if (filter instanceof Contains) {
                    out.write("geof:sfContains(");
                } else if (filter instanceof Crosses) {
                    out.write("geof:sfCrosses(");
                } else if (filter instanceof Disjoint) {
                    out.write("geof:sfDisjoint(");
                } else if (filter instanceof Equals) {
                    out.write("geof:sfEquals(");
                } else if (filter instanceof Intersects) {
                    out.write("geof:sfIntersects(");
                } else if (filter instanceof Overlaps) {
                    out.write("geof:sfOverlaps(");
                } else if (filter instanceof Touches) {
                    out.write("geof:sfTouches(");
                } else if (filter instanceof Within) {
                    out.write("geof:sfWithin(");
                } else {
                    throw new RuntimeException("Unknown operator: " + filter);
                }

                if (swapped) {
                    e2.accept(this, extraData);
                    out.write(", ");
                    e1.accept(this, extraData);
                } else {
                    e1.accept(this, extraData);
                    out.write(", ");
                    e2.accept(this, extraData);
                }

                out.write(")");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return extraData;
    }
    
    
    protected Object visitBinarySpatialOperator(
            BinarySpatialOperator filter,
            PropertyName property,
            Literal geometry,
            boolean swapped,
            Object extraData) {
            return visitBinarySpatialOperator(
                    filter, (Expression) property, (Expression) geometry, swapped, extraData);
    }
    
    
	
	protected FilterCapabilities createFilterCapabilities() {
        FilterCapabilities capabilities = new FilterCapabilities();

        // basic expressions
        capabilities.addType(Add.class);
        capabilities.addType(Subtract.class);
        capabilities.addType(Divide.class);
        capabilities.addType(Multiply.class);
        capabilities.addType(PropertyName.class);
        capabilities.addType(Literal.class);

        // basic filters
        capabilities.addAll(FilterCapabilities.LOGICAL_OPENGIS);
        capabilities.addAll(FilterCapabilities.SIMPLE_COMPARISONS_OPENGIS);
        capabilities.addType(PropertyIsNull.class);
        capabilities.addType(PropertyIsBetween.class);
        capabilities.addType(Id.class);
        capabilities.addType(IncludeFilter.class);
        capabilities.addType(ExcludeFilter.class);

        capabilities.addType(BBOX.class);
        capabilities.addType(Contains.class);
        capabilities.addType(Crosses.class);
        capabilities.addType(Disjoint.class);
        capabilities.addType(Equals.class);
        capabilities.addType(Intersects.class);
        capabilities.addType(Overlaps.class);
        capabilities.addType(Touches.class);
        capabilities.addType(Within.class);
        capabilities.addType(Beyond.class);
        
        return capabilities;
}
	
	/**
     * Write the SQL for an And filter
     *
     * @param filter the filter to visit
     * @param extraData extra data (unused by this method)
     */
    public Object visit(And filter, Object extraData) {
        return visit((BinaryLogicOperator) filter, "&&");
    }
    
	/**
     * Write the SQL for an And filter
     *
     * @param filter the filter to visit
     * @param extraData extra data (unused by this method)
     */
    public Object visit(Or filter, Object extraData) {
        return visit((BinaryLogicOperator) filter, "||");
    }
	
	
	@Override
	 public void encode(Filter filter) throws FilterToSQLException {
	        if (out == null) throw new FilterToSQLException("Can't encode to a null writer.");
	        if (getCapabilities().fullySupports(filter)) {

	            try {
	                if (!inline) {
	                    out.write("FILTER(");
	                }

	                filter.accept(this, null);

	                out.write(")");
	            } catch (java.io.IOException ioe) {
	                LOGGER.warning("Unable to export filter" + ioe);
	                throw new FilterToSQLException("Problem writing filter: ", ioe);
	            }
	        } else {
	            throw new FilterToSQLException("Filter type not supported: " + filter);
	        }
	    }

	public static void main(String[] args) throws CQLException, FilterToSQLException {
    	Filter filter = CQL.toFilter("attName >= 5 && attname <= 10");
    	Filter filter2 = CQL.toFilter( "b>=2 AND INTERSECTS(c, POINT(1 2)) AND CONTAINS(c,POINT(2 4))" );
    	Filter filter3=CQL.toFilter("INTERSECTS(ATTR1, GEOMETRYCOLLECTION (POINT (10 10),POINT (30 30),LINESTRING (15 15, 20 20)) )");
    	FilterToSPARQL ftosp=new FilterToSPARQL();
    	ftosp.encode(filter2);
    	System.out.println(ftosp.out.toString()+System.lineSeparator());
    	String prefixCollection="PREFIX geof: <http://www.opengis.net/def/function/geosparql/>\nPREFIX geo:<http://www.opengis.net/ont/geosparql#>\n";
    	String queryString="SELECT ?a ?b ?c WHERE {\n ?a ?b ?c . \n"+ftosp.out.toString()+"\n}";
    	System.out.println(prefixCollection+queryString);
    	Query query = QueryFactory.create(prefixCollection+queryString);
    	System.out.println(query.toString());
    }
	
}
