package org.codehaus.plexus.classworlds.realm;

/*
 * Copyright 2001-2006 Codehaus Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.codehaus.plexus.classworlds.ClassWorld;
import org.codehaus.plexus.classworlds.strategy.Strategy;
import org.codehaus.plexus.classworlds.strategy.StrategyFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The class loading gateway. Each class realm has access to a base class loader, imports form zero or more other class
 * loaders, an optional parent class loader and of course its own class path. When queried for a class/resource, a class
 * realm will always query its base class loader first before it delegates to a pluggable strategy. The strategy in turn
 * controls the order in which imported class loaders, the parent class loader and the realm itself are searched. The
 * base class loader is assumed to be capable of loading of the bootstrap classes.
 *
 * @author <a href="mailto:bob@eng.werken.com">bob mcwhirter</a>
 * @author Jason van Zyl
 */
public class ClassRealm
    extends URLClassLoader
{

    private ClassWorld world;

    private String id;

    private SortedSet<Entry> foreignImports;

    private SortedSet<Entry> parentImports;

    private Strategy strategy;

    private ClassLoader parentClassLoader;

    private static final boolean isParallelCapable = Closeable.class.isAssignableFrom( URLClassLoader.class );

    private String historyFileName;

    public final Set<String> recordedClassLoadOrder = Collections.synchronizedSet( new LinkedHashSet<String>() );

    private int recorderNumberOfClassPathElements;

    public final List<String> classLoadOrderFromFile = new ArrayList<String>();

    private int numberOfClassPathElementsFromFile;

    private final boolean shouldRecord;

    /**
     * Creates a new class realm.
     *
     * @param world           The class world this realm belongs to, must not be <code>null</code>.
     * @param id              The identifier for this realm, must not be <code>null</code>.
     * @param baseClassLoader The base class loader for this realm, may be <code>null</code> to use the bootstrap class
     *                        loader.
     */
    public ClassRealm( ClassWorld world, String id, ClassLoader baseClassLoader )
    {
        super( new URL[0], baseClassLoader );

        this.world = world;

        this.id = id;

        this.historyFileName = "classworld-" + id;

        foreignImports = new TreeSet<Entry>();

        strategy = StrategyFactory.getStrategy( this );

        shouldRecord = loadClassloadingHistory();

    }

    public String getId()
    {
        return this.id;
    }

    public ClassWorld getWorld()
    {
        return this.world;
    }

    public void importFromParent( String packageName )
    {
        if ( parentImports == null )
        {
            parentImports = new TreeSet<Entry>();
        }

        parentImports.add( new Entry( null, packageName ) );
    }

    boolean isImportedFromParent( String name )
    {
        if ( parentImports != null && !parentImports.isEmpty() )
        {
            for ( Entry entry : parentImports )
            {
                if ( entry.matches( name ) )
                {
                    return true;
                }
            }

            return false;
        }

        return true;
    }

    public void importFrom( String realmId, String packageName )
        throws NoSuchRealmException
    {
        importFrom( getWorld().getRealm( realmId ), packageName );
    }

    public void importFrom( ClassLoader classLoader, String packageName )
    {
        foreignImports.add( new Entry( classLoader, packageName ) );
    }

    public ClassLoader getImportClassLoader( String name )
    {
        for ( Entry entry : foreignImports )
        {
            if ( entry.matches( name ) )
            {
                return entry.getClassLoader();
            }
        }

        return null;
    }

    @SuppressWarnings( "UnusedDeclaration" )
    public Collection<ClassRealm> getImportRealms()
    {
        Collection<ClassRealm> importRealms = new HashSet<ClassRealm>();

        for ( Entry entry : foreignImports )
        {
            if ( entry.getClassLoader() instanceof ClassRealm )
            {
                importRealms.add( (ClassRealm) entry.getClassLoader() );
            }
        }

        return importRealms;
    }

    public Strategy getStrategy()
    {
        return strategy;
    }

    @SuppressWarnings( "UnusedDeclaration" )
    public void setParentClassLoader( ClassLoader parentClassLoader )
    {
        this.parentClassLoader = parentClassLoader;
    }

    public ClassLoader getParentClassLoader()
    {
        return parentClassLoader;
    }

    public void setParentRealm( ClassRealm realm )
    {
        this.parentClassLoader = realm;
    }

    public ClassRealm getParentRealm()
    {
        return ( parentClassLoader instanceof ClassRealm ) ? (ClassRealm) parentClassLoader : null;
    }

    public ClassRealm createChildRealm( String id )
        throws DuplicateRealmException
    {
        ClassRealm childRealm = getWorld().newRealm( id, null );

        childRealm.setParentRealm( this );

        return childRealm;
    }

    public void addURL( URL url )
    {
        String urlStr = url.toExternalForm();

        if ( urlStr.startsWith( "jar:" ) && urlStr.endsWith( "!/" ) )
        {
            urlStr = urlStr.substring( 4, urlStr.length() - 2 );

            try
            {
                url = new URL( urlStr );
            }
            catch ( MalformedURLException e )
            {
                e.printStackTrace();
            }
        }

        super.addURL( url );
        this.recorderNumberOfClassPathElements += 1;
        if ( recorderNumberOfClassPathElements == numberOfClassPathElementsFromFile && !shouldRecord )
        {
            startPreloader();
        }
    }

    // ----------------------------------------------------------------------
    // We delegate to the Strategy here so that we can change the behavior
    // of any existing ClassRealm.
    // ----------------------------------------------------------------------

    public Class<?> loadClass( String name )
        throws ClassNotFoundException
    {
        return loadClass( name, false );
    }

    protected Class<?> loadClass( String name, boolean resolve )
        throws ClassNotFoundException
    {

        if ( isParallelCapable )
        {
            return unsynchronizedLoadClass( name, resolve );

        }
        else
        {
            synchronized ( this )
            {
                return unsynchronizedLoadClass( name, resolve );
            }

        }
    }

    private Class<?> unsynchronizedLoadClass( String name, boolean resolve )
        throws ClassNotFoundException
    {
        if (shouldRecord) recordedClassLoadOrder.add( name );
        try
        {
            // first, try loading bootstrap classes
            return super.loadClass( name, resolve );
        }
        catch ( ClassNotFoundException e )
        {
            // next, try loading via imports, self and parent as controlled by strategy
            return strategy.loadClass( name );
        }
    }

    @Override
    public void close()
        throws IOException
    {
        if ( id != null && shouldRecord )
        { // Only write once ever
            writeClassLoadHistory();
        }
        super.close();
    }

    private void writeClassLoadHistory()
        throws IOException
    {
        FileWriter fw = new FileWriter( historyFileName );
        fw.write( Integer.toString( recorderNumberOfClassPathElements ) );
        fw.write( "\n" );

        for ( String loadedClass : recordedClassLoadOrder )
        {
            fw.write( loadedClass );
            fw.write( "\n" );
        }
        fw.close();
    }

    private void startPreloader()
    {
        Collections.reverse( classLoadOrderFromFile );
        int size = classLoadOrderFromFile.size();
        new Thread( getPreloader(classLoadOrderFromFile, size / 2, 0) ).start();
        new Thread( getPreloader(classLoadOrderFromFile, size -1 , size / 2 ) ).start();
    }

    private Runnable getPreloader(final List<String> elements, final int start, final int stop)
    {
        return new Runnable()
        {
            public void run()
            {
                long startTime = System.currentTimeMillis();
                for (int i = start; i >= stop; i--){
                    try
                    {
                        loadClass( elements.get( i ) );
                    }
                    catch ( ClassNotFoundException ignore )
                    {
                    }
                }
                System.out.println( "Preloading " + id + " Completed in " + ((int) System.currentTimeMillis() - startTime) );
            }
        };
    }

    private boolean loadClassloadingHistory()
    {

        try
        {
            File history = new File( historyFileName );
            if ( history.exists() )
            {
                FileInputStream fis = new FileInputStream( historyFileName );
                BufferedReader br = new BufferedReader( new InputStreamReader( fis, Charset.forName( "UTF-8" ) ) );
                String line;
                line = br.readLine();
                this.numberOfClassPathElementsFromFile = Integer.parseInt( line );

                while ( ( line = br.readLine() ) != null )
                {
                    classLoadOrderFromFile.add( line );
                }
                return false;
            }
        }
        catch ( NumberFormatException e )
        {
            System.out.println( "Malformed data file  " + id );
        }
        catch ( IOException ignore )
        {
        }
        return true;
    }

    protected Class<?> findClass( String name )
        throws ClassNotFoundException
    {
        /*
         * NOTE: This gets only called from ClassLoader.loadClass(Class, boolean) while we try to check for bootstrap
         * stuff. Don't scan our class path yet, loadClassFromSelf() will do this later when called by the strategy.
         */
        throw new ClassNotFoundException( name );
    }

    public URL findResource( String name )
    {
        /*
         * NOTE: If this gets called from ClassLoader.getResource(String), delegate to the strategy. If this got called
         * directly by other code, only scan our class path as usual for an URLClassLoader.
         */
        StackTraceElement caller = new Exception().getStackTrace()[1];

        if ( "java.lang.ClassLoader".equals( caller.getClassName() ) )
        {
            return strategy.getResource( name );
        }
        else
        {
            return super.findResource( name );
        }
    }

    public Enumeration<URL> findResources( String name )
        throws IOException
    {
        /*
         * NOTE: If this gets called from ClassLoader.getResources(String), delegate to the strategy. If this got called
         * directly by other code, only scan our class path as usual for an URLClassLoader.
         */
        StackTraceElement caller = new Exception().getStackTrace()[1];

        if ( "java.lang.ClassLoader".equals( caller.getClassName() ) )
        {
            return strategy.getResources( name );
        }
        else
        {
            return super.findResources( name );
        }
    }

    // ----------------------------------------------------------------------------
    // Display methods
    // ----------------------------------------------------------------------------

    public void display()
    {
        display( System.out );
    }

    public void display( PrintStream out )
    {
        out.println( "-----------------------------------------------------" );

        for ( ClassRealm cr = this; cr != null; cr = cr.getParentRealm() )
        {
            out.println( "realm =    " + cr.getId() );
            out.println( "strategy = " + cr.getStrategy().getClass().getName() );

            showUrls( cr, out );

            out.println();
        }

        out.println( "-----------------------------------------------------" );
    }

    private static void showUrls( ClassRealm classRealm, PrintStream out )
    {
        URL[] urls = classRealm.getURLs();

        for ( int i = 0; i < urls.length; i++ )
        {
            out.println( "urls[" + i + "] = " + urls[i] );
        }

        out.println( "Number of foreign imports: " + classRealm.foreignImports.size() );

        for ( Entry entry : classRealm.foreignImports )
        {
            out.println( "import: " + entry );
        }

        if ( classRealm.parentImports != null )
        {
            out.println( "Number of parent imports: " + classRealm.parentImports.size() );

            for ( Entry entry : classRealm.parentImports )
            {
                out.println( "import: " + entry );
            }
        }
    }

    public String toString()
    {
        return "ClassRealm[" + getId() + ", parent: " + getParentClassLoader() + "]";
    }

    //---------------------------------------------------------------------------------------------
    // Search methods that can be ordered by strategies to load a class
    //---------------------------------------------------------------------------------------------

    public Class<?> loadClassFromImport( String name )
    {
        ClassLoader importClassLoader = getImportClassLoader( name );

        if ( importClassLoader != null )
        {
            try
            {
                return importClassLoader.loadClass( name );
            }
            catch ( ClassNotFoundException e )
            {
                return null;
            }
        }

        return null;
    }

    public Class<?> loadClassFromSelf( String name )
    {
        synchronized ( getClassRealmLoadingLock( name ) )
        {
            try
            {
                Class<?> clazz = findLoadedClass( name );

                if ( clazz == null )
                {
                    clazz = super.findClass( name );
                }

                return clazz;
            }
            catch ( ClassNotFoundException e )
            {
                return null;
            }
        }
    }

    private Object getClassRealmLoadingLock( String name )
    {
        if ( isParallelCapable )
        {
            return getClassLoadingLock( name );
        }
        else
        {
            return this;
        }
    }

    public Class<?> loadClassFromParent( String name )
    {
        ClassLoader parent = getParentClassLoader();

        if ( parent != null && isImportedFromParent( name ) )
        {
            try
            {
                return parent.loadClass( name );
            }
            catch ( ClassNotFoundException e )
            {
                return null;
            }
        }

        return null;
    }

    //---------------------------------------------------------------------------------------------
    // Search methods that can be ordered by strategies to get a resource
    //---------------------------------------------------------------------------------------------

    public URL loadResourceFromImport( String name )
    {
        ClassLoader importClassLoader = getImportClassLoader( name );

        if ( importClassLoader != null )
        {
            return importClassLoader.getResource( name );
        }

        return null;
    }

    public URL loadResourceFromSelf( String name )
    {
        return super.findResource( name );
    }

    public URL loadResourceFromParent( String name )
    {
        ClassLoader parent = getParentClassLoader();

        if ( parent != null && isImportedFromParent( name ) )
        {
            return parent.getResource( name );
        }
        else
        {
            return null;
        }
    }

    //---------------------------------------------------------------------------------------------
    // Search methods that can be ordered by strategies to get resources
    //---------------------------------------------------------------------------------------------

    public Enumeration<URL> loadResourcesFromImport( String name )
    {
        ClassLoader importClassLoader = getImportClassLoader( name );

        if ( importClassLoader != null )
        {
            try
            {
                return importClassLoader.getResources( name );
            }
            catch ( IOException e )
            {
                return null;
            }
        }

        return null;
    }

    public Enumeration<URL> loadResourcesFromSelf( String name )
    {
        try
        {
            return super.findResources( name );
        }
        catch ( IOException e )
        {
            return null;
        }
    }

    public Enumeration<URL> loadResourcesFromParent( String name )
    {
        ClassLoader parent = getParentClassLoader();

        if ( parent != null && isImportedFromParent( name ) )
        {
            try
            {
                return parent.getResources( name );
            }
            catch ( IOException e )
            {
                // eat it
            }
        }

        return null;
    }

    static
    {
        if ( isParallelCapable ) // Avoid running this method on older jdks
        {
            registerAsParallelCapable();
        }
    }

}
