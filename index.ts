import {omit, pullAt, isEqual} from 'lodash';

type TData = { [key: string]: Rows}
type TColMap = { [key: string]: string}

interface IDataframe {
    getByCol(string) : Rows
    addCol(string, Rows): void
    print():void
    removeDuplicates(string, boolean):void
    length():number
    columns():string[]
}
class Rows {
    private data = []
    constructor(values = []) {
        this.data = values
    }
    add(values = []) {
        if(this.add.length !== values.length) {
            throw 'Unsupported length'
        }
        return this.data.map((r, i) => r + values[i])
    }
    push(v) {
        this.data.push(v)
        return this
    }
    extend(vals :any[] = []) {
        this.data = [...this.data, ...vals]
    }
    keepOnly(indices = []) {
        this.data = pullAt(this.data, indices)
    }
    toArray():any[] {
        return this.data
    }
    length():number {
        return this.data.length
    }
    at(index: number):any {
        return this.data[index]
    }
}

export default class Dataframe implements IDataframe{
    private cols_map: TColMap = {};
    private data: TData = {};
    constructor(values=[]) {
        if(values.length > 0 && typeof values[0] == 'object') {
            values.forEach((row, i) => {
                const cols = Object.keys(row);
                cols.forEach((c,i) => {
                    this.cols_map[c] = c;
                    if(!this.data[c]) {
                        this.data[c] = new Rows()
                    }
                    this.data[c].push(row[c])
                })
            })
        }
        return this
    }
    getByCol(col:string = ''):Rows {
        if(!this.cols_map[col]) {
            throw `Invalid column ${col}` 
        }
        return this.data[col]
    }
    getByCols(cols:string[] = []):any [] {
        return cols.map(c => this.getByCol(c))
    }
    addCol(col:string = "", data) {
        this.cols_map[col] = col
        this.data[col] = new Rows(data)
    }
    deleteCol(col = '') {
        if(!this.cols_map[col]) {
            throw `Column ${col} doesnt exists`
        } 
        this.cols_map = omit(this.cols_map, col)
        this.data = omit(this.data, col)
    }
    rename(old = '', new_col='') {
        if(!this.cols_map[old]) {
            throw `Column ${old} doesnt exists`
        }
        if(old !== new_col && this.cols_map[new_col]) {
            throw 'Duplicate column names'
        }
        this.addCol(new_col, new Rows(this.getByCol(old).toArray()))
        this.deleteCol(old)
    }
    print() {
        console.table(this.data)
    }
    private reArrangeWithIndices(indices = []) {
        Object.keys(this.data).forEach(key => {
            this.data[key].keepOnly(indices)
        })
    }
    removeDuplicates(on:string = '', keepLast:boolean = false) {
        if(!this.cols_map[on]) {
            throw `Column ${on} doesnt exists`
        }
        const data_index_map:{[key:string]: number} = {}
        const data = this.getByCol(on).toArray()
        data.forEach((r, i) => {
            const shouldUpdate = keepLast ? true : data_index_map[r] === undefined
            if(shouldUpdate) {
                data_index_map[r] = i
            }
        })
        this.reArrangeWithIndices(Object.keys(data_index_map).map((k:string):number => data_index_map[k]))

    }
    length():number{
        const temp_col = Object.keys(this.cols_map)[0]
        return this.data[temp_col].length()
    }
    columns():string[]{
        return Object.keys(this.cols_map)
    }
    locateByIndex(index: number) {
        if(index >= this.length()) {
            throw 'Index out of bounds'
        }
        return this.columns().reduce((acc, k) => ({...acc, [k]: this.data[k].at(index)}), {})
    }
    locateByIndices(indices: number[] = []): any[] {
        return indices.map(r => this.locateByIndex(r))
    }
    extend(data: any[]) {
        const newDf = new Dataframe(data)
        if(!isEqual(this.columns(), newDf.columns())) {
            throw 'Columns doesnt match to append'
        }
        this.columns().forEach(c => {
            this.data[c].extend(newDf.getByCol(c).toArray())
        })
    }
    groupBy(on:string = '') {
        if(!this.cols_map[on]) {
            throw `Column ${on} doesnt exists`
        }
        const data_index_map: {[key:string]: number[]} = {}
        const response: {[key:string]: IDataframe} = {}

        this.getByCol(on).toArray().forEach((r, i) => {
            if(data_index_map[r] === undefined) {
                data_index_map[r] = []
            }
            data_index_map[r].push(i)
        })
        Object.keys(data_index_map).forEach(k => {
            response[k] = new Dataframe(this.locateByIndices(data_index_map[k]))
        })
        return response
    }
}
